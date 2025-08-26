"""
suggested process for analysis wrapper capsule:
    process = construct_processing_record(dispatch_inputs)
    if not docdb_record_exists(process):
    ... run processing
    write_results_and_metadata(process, ANALYSIS_BUCKET)
"""

import logging
import os
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

import aind_data_schema.core.processing as ps
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_schema.components.identifiers import CombinedData, DataAsset
from aind_data_schema.core.metadata import Metadata
from codeocean import CodeOcean
from codeocean.computation import Computation, PipelineProcess

from .analysis_dispatch_model import AnalysisDispatchModel
from .result_files import (
    copy_results_to_s3,
    create_results_metadata,
    processing_prefix,
)

PARAM_PREFIX = "param_"


def extract_parameters(
    process: PipelineProcess | Computation,
) -> Dict[str, Any]:
    """Extract parameters for a specific capsule from computation processes.

    Args:
        process: PipelineProcess or Computation object containing parameters

    Returns:
        Dict[str, Any]: Parameters specific to the target capsule
    """
    if not process.parameters:
        return {}
    return {
        param.name if param.name else f"{PARAM_PREFIX}{i}": param.value
        for i, param in enumerate(process.parameters)
        if param.value
    }


def construct_processing_record(
    dispatch_inputs: AnalysisDispatchModel,
    **kwargs,
) -> ps.DataProcess:
    """Construct a processing record by
       combining Code Ocean metadata with analysis job data.

    Args:
        dispatch_inputs: AnalysisDispatchModel
        containing analysis metadata including:
            - s3_location (str): S3 URL for input data
        **kwargs: Additional parameters passed to the process

    Returns:
        ps.DataProcess: Constructed processing record with combined metadata
    """
    process = query_code_ocean_metadata()
    # add s3_location and parameters from analysis_job_dict

    new_inputs = [DataAsset(url=url) for url in dispatch_inputs.s3_location]
    if process.code.input_data is None:
        process.code.input_data = new_inputs
    else:
        process.code.input_data.extend(new_inputs)

    # add file location as a tracked parameter
    if dispatch_inputs.file_location:
        kwargs.update(file_location=dispatch_inputs.file_location)

    # TODO: allow additional parameters from the dispatch also?
    process.code.parameters = process.code.parameters.model_copy(update=kwargs)

    return process


def _initialize_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client using environment variables.

    Returns:
        CodeOcean: Initialized Code Ocean client

    Raises:
        ValueError: If required environment variables are missing
    """
    domain = (
        os.getenv("CODEOCEAN_DOMAIN") or "codeocean.allenneuraldynamics.org"
    )
    token = os.getenv("CODEOCEAN_API_TOKEN")

    if not token:
        raise ValueError(
            "CODEOCEAN_API_TOKEN environment variable is required"
        )

    return CodeOcean(domain=f"https://{domain}", token=token)


def query_code_ocean_metadata(
    capsule_id: Optional[str] = None,
) -> ps.DataProcess:
    """
    Query Code Ocean API for metadata
    about the current analysis pipeline or capsule run.

    """
    # Initialize the Code Ocean client and get computation ID
    client = _initialize_codeocean_client()
    computation_id = os.getenv("CO_COMPUTATION_ID")

    # Get the current computation details
    computation = client.computations.get_computation(computation_id)

    # Extract relevant metadata from the computation
    process = ps.DataProcess.model_construct(
        # computation.name likely only set for named runs
        process_type=ps.ProcessName.ANALYSIS,
        process_stage=ps.ProcessStage.ANALYSIS,
        start_date_time=datetime.fromtimestamp(computation.created),
        end_date_time=datetime.fromtimestamp(
            computation.created + computation.run_time
        ),
    )
    # not sure if this is the best way to get this info
    # but not recorded in computation object?
    pipeline = client.capsules.get_capsule(
        os.getenv("CO_PIPELINE_ID") or os.getenv("CO_CAPSULE_ID")
    )
    # TODO: this owner attribute is just a CO UUID
    process.experimenters = [pipeline.owner]
    process.name = pipeline.name

    parameters = extract_parameters(computation)
    # find the component process for this capsule or specified capsule ID
    capsule_id = capsule_id or os.getenv("CO_CAPSULE_ID")
    if not capsule_id:
        raise ValueError(
            "Capsule ID must be provided or "
            "set in environment variable CO_CAPSULE_ID"
        )
    if computation.processes:
        component_process = next(
            proc
            for proc in computation.processes
            if proc.capsule_id == capsule_id
        )
        parameters.update(extract_parameters(component_process))
        # version = str(component_process.version)
    capsule = client.capsules.get_capsule(capsule_id)
    if computation.data_assets:
        input_data = [
            DataAsset(url=get_data_asset_url(client, asset.id))
            for asset in computation.data_assets
        ]
    else:
        input_data = []
    code = ps.Code(
        url=capsule.cloned_from_url or "",
        name=capsule.name or "",
        version=get_version_from_git_remote(capsule.slug),
        run_script="code/run",
        parameters=parameters,
        input_data=input_data,
    )

    process.code = code
    return process


def _run_git_command(command: List[str]) -> str:
    """Run a git command safely and return its output.

    Args:
        command: List of command arguments

    Returns:
        str: Command output or default value if command fails
    """
    result = subprocess.run(
        command, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def get_code_metadata_from_git() -> ps.Code:
    """Get git metadata for the current repository.

    Returns:
        ps.Code: Code object with git metadata
    """
    # Use safer command list format instead of shell=True
    git_remote_url = _run_git_command(["git", "remote", "get-url", "origin"])
    git_commit_hash = _run_git_command(["git", "rev-parse", "HEAD"])

    # Get repo name from remote URL instead of shell command
    repo_name = (
        git_remote_url.split("/")[-1].replace(".git", "")
        if git_remote_url
        else ""
    )

    code = ps.Code(
        # git remote URL from shell command
        url=git_remote_url,
        version=git_commit_hash,
        run_script="code/run",
        name=repo_name,
    )
    return code


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
            username = os.getenv("CODEOCEAN_EMAIL") or _run_git_command(
                ["git", "config", "user.email"]
            )
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


def get_version_from_git_remote(capsule_slug: str) -> str:
    """Get the git version for a specific capsule from the remote repository.

    Args:
        capsule_slug: Slug ID of the capsule to
        retrieve metadata for (7-digit numeric ID)

    Returns:
        str: Commit hash of the HEAD of the capsule's git repository
    """

    git_remote_url = f"{_get_git_remote_url()}/capsule-{capsule_slug}.git"
    git_commit_hash = _run_git_command(
        ["git", "ls-remote", git_remote_url, "HEAD"]
    )
    if not git_commit_hash:
        raise ValueError(
            f"Could not retrieve git commit hash for capsule {capsule_slug}"
        )
    return git_commit_hash.split()[0]  # Return the commit hash part


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
            "Data asset source bucket "
            f"{data_asset.source_bucket} not supported."
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


def docdb_record_exists(processing: ps.DataProcess) -> bool:
    """
    Check the document database for
    whether a record already exists matching the analysis metadata

    Args:
        processing: Processing record to check

    Returns:
        True if record exists or False if not
    """
    responses = get_docdb_records(processing)

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


def get_docdb_records(processing: ps.DataProcess) -> List[Dict[str, Any]]:
    """
    Get the document database record for the given processing object

    Args:
        processing: Processing record to check

    Returns:
        List of dictionary records
    """
    client: MetadataDbClient = get_docdb_client()

    docdb_id = processing_prefix(processing)
    filter_query = {"_id": docdb_id}

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
        [asset.model_dump(mode="json") for asset in input_data]
        if input_data
        else None
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


def get_docdb_client(
    host=None, database=None, collection=None
) -> MetadataDbClient:
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
    process: ps.DataProcess, s3_bucket: str
) -> None:
    """
    Writes output and copies to s3.
    Process record is written to docdb

    Args:
        process: Processing record
        s3_bucket: Bucket to copy results to

    """
    metadata, docdb_id = create_results_metadata(process, s3_bucket)
    with open("/results/metadata.nd.json", "w") as f:
        f.write(metadata.model_dump_json(indent=2))
    copy_results_to_s3(metadata)
    write_to_docdb(metadata, docdb_id)
