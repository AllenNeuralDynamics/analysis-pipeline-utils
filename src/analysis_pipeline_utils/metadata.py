"""Utility functions for handling metadata related operations"""

import logging
import os
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

import aind_data_schema.core.processing as ps
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.utils import get_record_from_docdb
from aind_data_schema.components.identifiers import CombinedData, DataAsset
from aind_data_schema.core.metadata import Metadata
from codeocean import CodeOcean
from codeocean.computation import Computation, PipelineProcess
from codeocean.capsule import Capsule

from .analysis_dispatch_model import AnalysisDispatchModel
from .result_files import (
    copy_results_to_s3,
    create_results_metadata,
    processing_prefix,
)


def get_metadata_for_records(
    analysis_dispatch_input: AnalysisDispatchModel,
) -> List[Dict]:
    """
    Retrieves metadata from DocDB for records specified
    by analysis dispatch input

    Parameters
    ----------
    analysis_dispatch_input: AnalysisDispatchModel
        The analysis dispatch input to fetch metadata for

    Returns
    -------
    List[Dict]

    The list of metadata dictionaries for each record in
    the dispatch input
    """
    record_ids = analysis_dispatch_input.docdb_record_id
    metadata_records = []
    docdb_client = get_docdb_client()

    for record_id in record_ids:
        record = get_record_from_docdb(docdb_client, record_id)
        if not record:
            logging.warning(f"No record found for id {record_id}. Skipping adding this")
            continue

        metadata_records.append(record)

    return metadata_records


def extract_parameters(
    process: PipelineProcess | Computation,
) -> Dict[str, Any]:
    """Extract parameters for a specific capsule from computation processes.

    Args:
        process: PipelineProcess or Computation object containing parameters

    Returns:
        Dict[str, Any]: Parameters specific to the target capsule
    """

    num_prefix = "ordered_param_"
    if not process.parameters:
        return {}
    return {
        param.name if param.name else f"{num_prefix}{i}": param.value
        for i, param in enumerate(process.parameters)
        if param.value
    }


def construct_processing_record(
    process: ps.DataProcess,
    dispatch_inputs: AnalysisDispatchModel,
    **params,
) -> ps.DataProcess:
    """Construct a processing record by
       combining Code Ocean metadata with analysis job data.

    Args:
        dispatch_inputs: AnalysisDispatchModel
        containing analysis metadata including:
            - s3_location (str): S3 URL for input data
        **params: Additional parameters passed to the process

    Returns:
        ps.DataProcess: Constructed processing record with combined metadata
    """

    # add s3_location and parameters from analysis_job_dict
    new_inputs = [DataAsset(url=url) for url in dispatch_inputs.s3_location]
    if process.code.input_data is None:
        process.code.input_data = new_inputs
    else:
        process.code.input_data.extend(new_inputs)

    # remove dry_run from parameters
    params.pop("dry_run", None)

    # add file location as a tracked parameter
    if dispatch_inputs.file_location:
        params.update(file_location=dispatch_inputs.file_location)

    if dispatch_inputs.query:
        if process.notes is None:
            process.notes = ""
        else:
            process.notes += "\n"
        process.notes += f"Query used to retrieve data assets: {dispatch_inputs.query}"

    if dispatch_inputs.analysis_code:
        old_params = dispatch_inputs.analysis_code.parameters.model_dump()
    else:
        old_params = {}

    if dispatch_inputs.distributed_parameters:
        distributed_params = dispatch_inputs.distributed_parameters
    else:
        distributed_params = {}

    process.code.parameters = process.code.parameters.model_copy(
        update=(old_params | params | distributed_params)
    )
    return process


def _initialize_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client using environment variables.

    Returns:
        CodeOcean: Initialized Code Ocean client

    Raises:
        ValueError: If required environment variables are missing
    """
    domain = os.getenv("CODEOCEAN_DOMAIN") or "codeocean.allenneuraldynamics.org"
    token = os.getenv("CODEOCEAN_API_TOKEN")

    if not token:
        raise ValueError("CODEOCEAN_API_TOKEN environment variable is required")

    return CodeOcean(domain=f"https://{domain}", token=token)


def get_codeocean_process_metadata(
    capsule_id: Optional[str] = None,
    computation_id: Optional[str] = None,
    from_dispatch: bool = False,
) -> ps.DataProcess:
    """
    Query Code Ocean API for metadata
    about the current analysis pipeline or capsule run.

    """
    # Initialize the Code Ocean client and get computation ID
    client = _initialize_codeocean_client()
    computation_id = computation_id or os.getenv("CO_COMPUTATION_ID")
    capsule_id = capsule_id or os.getenv("CO_CAPSULE_ID")

    # Get the current computation details
    computation = client.computations.get_computation(computation_id)

    # Extract relevant metadata from the computation
    process = ps.DataProcess.model_construct(
        # computation.name likely only set for named runs
        experimenters=[os.getenv("CODEOCEAN_EMAIL", "unknown")],
        process_type=ps.ProcessName.ANALYSIS,
        stage=ps.ProcessStage.ANALYSIS,
        start_date_time=datetime.fromtimestamp(computation.created),
        end_date_time=datetime.fromtimestamp(
            computation.created + computation.run_time
        ),
    )

    # find the component process for the capsule
    if not capsule_id:
        raise ValueError(
            "Capsule ID must be provided or set in environment variable CO_CAPSULE_ID"
        )
    version = None
    if computation.processes:
        for i, proc in enumerate(computation.processes):
            # if run from dispatch capsule, match the other process in the pipeline
            if (proc.capsule_id == capsule_id) ^ from_dispatch:
                parameters = extract_parameters(proc)
                version = proc.version
                break
    elif not from_dispatch:
        parameters = extract_parameters(computation)
    else:
        parameters = {}

    capsule = client.capsules.get_capsule(capsule_id)
    process.name = capsule.name
    if not version:
        version = get_capsule_version(capsule, os.getenv("CO_CAPSULE_BRANCH", "HEAD"))

    if computation.data_assets:
        input_data = [
            DataAsset(url=get_data_asset_url(client, asset.id))
            for asset in computation.data_assets
        ]
    else:
        input_data = []

    code = ps.Code(
        name=capsule.name,
        url=get_capsule_url(capsule),
        version=version,
        run_script="code/run",
        parameters=parameters,
        input_data=input_data,
    )

    process.code = code
    return process.model_validate(process)


def _run_git_command(command: List[str]) -> str:
    """Run a git command safely and return its output.

    Args:
        command: List of command arguments

    Returns:
        str: Command output or default value if command fails
    """
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _get_git_remote_url() -> str:
    """Get the git remote URL for the current repository.

    Returns:
        str: Remote URL of the git repository
    """
    # these variables are set in pipelines only
    credentials = os.getenv("GIT_ACCESS_TOKEN")
    domain = os.getenv("GIT_HOST")
    if not all([credentials, domain]):
        try:
            username = os.getenv("CODEOCEAN_EMAIL")
            username = username.replace("@", "%40")
            token = os.getenv("CODEOCEAN_API_TOKEN")
            credentials = f"{username}:{token}"
            domain = os.getenv("CODEOCEAN_DOMAIN")
        except Exception:
            raise ValueError(
                "GIT_ACCESS_TOKEN or CODEOCEAN_API_TOKEN "
                "environment variable is required"
            )
    return f"https://{credentials}@{domain}"


def get_capsule_version(capsule: Capsule, branch="HEAD") -> str:
    """Get the git version for a specific capsule from the remote repository.

    Args:
        capsule: Capsule object

    Returns:
        str: Commit hash of the HEAD of the capsule's git repository
    """
    capsule_slug = capsule.slug
    git_remote_url = f"{_get_git_remote_url()}/capsule-{capsule_slug}.git"
    git_commit_hash = _run_git_command(["git", "ls-remote", git_remote_url, branch])
    if not git_commit_hash:
        raise ValueError(
            f"Could not retrieve git commit hash for capsule {capsule_slug}"
        )
    return git_commit_hash.split()[0]  # Return the commit hash part


def get_capsule_url(capsule: Capsule) -> str:
    """Get the URL for a specific capsule.

    Args:
        capsule: Capsule object
    Returns:
        str: URL of the capsule
    """
    domain = os.getenv("CODEOCEAN_DOMAIN") or "codeocean.allenneuraldynamics.org"
    return f"https://{domain}/capsule/{capsule.slug}"


def get_data_asset_url(client: CodeOcean, data_asset_id: str) -> str:
    """Get the S3 URL for a data asset.

    Args:
        client: CodeOcean client instance
        data_asset_id: ID of the data asset

    Returns:
        str: S3 URL for the data asset

    Raises:
        ValueError: If data asset origin is not AWS
    """
    data_asset = client.data_assets.get_data_asset(data_asset_id)
    if data_asset.source_bucket and data_asset.source_bucket.origin == "aws":
        bucket = data_asset.source_bucket.bucket
        prefix = data_asset.source_bucket.prefix or ""
        return f"s3://{bucket}/{prefix}"
    else:
        raise ValueError(
            f"Data asset source bucket {data_asset.source_bucket} not supported."
        )


def write_to_docdb(metadata: Metadata, hash: str):
    """
    Write the processing record to the document database

    Args:
        metadata: Metadata record to be written
        hash: str hash on processing.code
    """
    client = get_docdb_client()
    metadata_dump = metadata.model_dump(mode="json")
    metadata_dump["_id"] = hash  # Ensure a unique ID for the record
    response = client.insert_one_docdb_record(metadata_dump)
    return response


def docdb_record_exists(process_code: ps.Code) -> bool:
    """
    Check the document database for
    whether a record already exists matching the analysis metadata

    Args:
        process_code: Processing code record to check

    Returns:
        True if record exists or False if not
    """
    responses = get_docdb_records(process_code)

    if len(responses) == 1:
        return True
    elif len(responses) > 1:
        logging.warning(
            "Multiple records found in document database. "
            "This indicates a potential data integrity issue."
        )
        return True
    else:
        return False


def get_docdb_records(process_code: ps.Code) -> List[Dict[str, Any]]:
    """
    Get the document database record for the given processing object

    Args:
        process_code: Processing code record to check

    Returns:
        List of dictionary records
    """
    client: MetadataDbClient = get_docdb_client()

    docdb_id = processing_prefix(process_code)
    filter_query = {"name": docdb_id}

    responses = client.retrieve_docdb_records(filter_query=filter_query)
    return responses


def get_docdb_records_partial(
    latest_only=False,
    code_url: Optional[str] = None,
    code_version: Optional[str] = None,
    input_data_locations: Optional[List[str]] = None,
    input_data: Optional[List[DataAsset | CombinedData]] = None,
    parameters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Get the document database record matching specified properties
    """
    client: MetadataDbClient = get_docdb_client()

    if input_data_locations:
        input_data = [DataAsset(url=url) for url in input_data_locations]
    input_data_dict = (
        [asset.model_dump(mode="json") for asset in input_data] if input_data else None
    )

    code_prefix = "processing.data_processes.0.code"
    filter_query = {}
    if code_url:
        filter_query[f"{code_prefix}.url"] = code_url
    if code_version:
        filter_query[f"{code_prefix}.version"] = code_version
    if input_data_dict:
        filter_query[f"{code_prefix}.input_data"] = {"$all": input_data_dict}
    if parameters:
        filter_query[f"{code_prefix}.parameters"] = parameters
    if latest_only:
        pipeline = [
            {"$match": filter_query},
            {"$sort": {"created": -1}},
            {
                "$group": {
                    "_id": "$processing.data_processes.0.code",
                    "latest_record": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$latest_record"}},
        ]
        responses = client.aggregate_docdb_records(pipeline=pipeline)
    else:
        responses = client.retrieve_docdb_records(filter_query=filter_query)
    return responses


def get_docdb_client(host=None, database=None, collection=None) -> MetadataDbClient:
    """
    Get a client for the document database
    """
    if host is None:
        host = os.getenv("DOCDB_HOST")
    if database is None:
        database = os.getenv("DOCDB_DATABASE")
    if collection is None:
        collection = os.getenv("DOCDB_COLLECTION")
    client = MetadataDbClient(
        host=host,
        database=database,
        collection=collection,
    )
    return client


def write_results_and_metadata(
    process: ps.DataProcess,
    s3_bucket: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Writes output and copies to s3.
    Process record is written to docdb

    Args:
        process: Processing record
        s3_bucket: Bucket to copy results to

    """
    if s3_bucket is None:
        s3_bucket = os.getenv("ANALYSIS_BUCKET")
    metadata, docdb_id = create_results_metadata(process, s3_bucket)
    with open("/results/metadata.nd.json", "w") as f:
        f.write(metadata.model_dump_json(indent=2))
    if not dry_run:
        copy_results_to_s3(metadata)
        write_to_docdb(metadata, docdb_id)
