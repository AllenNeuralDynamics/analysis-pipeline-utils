"""Utility functions for handling metadata related operations"""

import copy
import logging
import os
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import aind_data_schema.core.processing as ps
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.utils import get_record_from_docdb
from aind_data_schema.components.identifiers import CombinedData, DataAsset
from aind_data_schema.core.metadata import Metadata
from codeocean import CodeOcean
from codeocean.computation import Computation, PipelineProcess
from codeocean.capsule import Capsule

from .analysis_dispatch_model import AnalysisDispatchModel
from .git import (
    _initialize_codeocean_client,
    get_capsule_commit_hash,
    get_capsule_version_ignoring_patches,
)
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
    docdb_client = MetadataDbClient(host=os.getenv("DOCDB_HOST"))

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


def update_analysis_process(
    process: ps.DataProcess,
    dispatch_inputs: AnalysisDispatchModel,
    **params,
) -> ps.DataProcess:
    """Construct an analysis process record by
       combining Code Ocean metadata with analysis job data.

    Args:
        process: Base process record to build on. Left unmodified.
        dispatch_inputs: AnalysisDispatchModel containing analysis metadata including:
            - s3_location (str): S3 URL for input data
        **params: Additional parameters passed to the process

    Returns:
        ps.DataProcess: analysis process record with combined metadata
    """

    # Callers fetch one base record per capsule and reuse it for every job, so
    # the updates below have to land on a copy. Applied in place they would
    # accumulate: input_data grows job by job and parameters carry over from
    # the previous job. Both feed processing_prefix(), so the record hash used
    # to detect already-processed jobs would drift after the first job.
    process = copy.deepcopy(process)

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


def analysis_pipeline_processing_metadata(
    base_process: ps.DataProcess,
) -> ps.Processing:
    """Construct a processing record for the analysis pipeline run,
    adding pipeline metadata if available.

     Args:
        base_process: The base process record to update with pipeline metadata
    Returns:
        ps.Processing: The updated processing record with pipeline metadata
    """
    processing = ps.Processing.create_with_sequential_process_graph(
        data_processes=[base_process]
    )
    pipeline_id = os.getenv("CO_PIPELINE_ID")
    if pipeline_id:
        pipeline_process = get_codeocean_process_metadata(
            capsule_id=pipeline_id, extra_params_from_process_name="dispatch"
        )
        processing.data_processes[0].pipeline_name = pipeline_process.name
        processing.pipelines = [pipeline_process.code]
    return processing.model_validate(processing)


def get_codeocean_process_metadata(
    computation_id: Optional[str] = None,
    capsule_id: Optional[str] = None,
    capsule_name: Optional[str] = None,
    extra_params_from_process_name: Optional[str] = None,
) -> ps.DataProcess:
    """
    Query Code Ocean API for metadata
    about the current analysis pipeline or capsule run.

    """
    # Initialize the Code Ocean client and get computation ID
    client = _initialize_codeocean_client()
    computation_id = computation_id or os.getenv("CO_COMPUTATION_ID")
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

    if capsule_id is None and capsule_name is None:
        capsule_id = os.getenv("CO_CAPSULE_ID")
        if capsule_id is None:
            raise ValueError(
                "capsule_id or environment variable CO_CAPSULE_ID must be provided"
            )

    version = None
    # find the component process for the capsule
    if computation.processes:
        parameters = {}
        # ok to not match, capsule_id may be for pipeline not component
        # (in this case there seems to be no explicit record of the version run!?)
        proc = _get_matching_computation_subprocess(
            computation, capsule_id, capsule_name
        )
        if proc:
            parameters = extract_parameters(proc)
            # PipelineProcess only provides MAJOR; Code.version requires MAJOR.MINOR.
            # See https://github.com/codeocean/codeocean-sdk-python/issues/74.
            version = f"{proc.version}.0" if proc.version is not None else None
            capsule_id = proc.capsule_id
        if extra_params_from_process_name:
            extra_proc = _get_matching_computation_subprocess(
                computation,
                capsule_id=None,
                capsule_name=extra_params_from_process_name,
            )
            if extra_proc:
                extra_parameters = extract_parameters(extra_proc)
                parameters.update(extra_parameters)
    else:  # not a pipeline run, get parameters from computation level
        parameters = extract_parameters(computation)
    if capsule_id is None:
        raise ValueError("Could not find matching process for capsule ID")

    capsule = client.capsules.get_capsule(capsule_id)
    process.name = capsule.name
    if not version:
        branch = os.getenv("CO_CAPSULE_BRANCH", "HEAD")
        patch_list = os.getenv("PATCH_COMMITS")
        version = get_capsule_version_ignoring_patches(
            capsule, patch_list=patch_list, branch=branch
        )
        hash = get_capsule_commit_hash(capsule, branch)
        version = version or hash
        process.notes = f"Git commit hash: {hash}"

    if computation.data_assets:
        input_data = [
            DataAsset(url=get_data_asset_url(client, asset.id))
            for asset in computation.data_assets
        ]
    else:
        input_data = []

    process.code = ps.Code(
        name=capsule.name,
        url=get_capsule_url(capsule),
        version=version,
        run_script="code/run",
        parameters=parameters,
        input_data=input_data,
    )
    return process.model_validate(process)


def _get_matching_computation_subprocess(
    computation: Computation,
    capsule_id: Optional[str],
    capsule_name: Optional[str],
    raise_on_not_found: bool = False,
) -> Optional[PipelineProcess]:
    """Helper function to find the subprocess within a computation
    that matches either the capsule ID or name."""
    if not computation.processes:
        return None
    matched_process = [
        proc
        for proc in computation.processes
        if (proc.capsule_id == capsule_id)
        or (capsule_name is not None and capsule_name in proc.name)
    ]
    if len(matched_process) == 0:
        if raise_on_not_found:
            raise ValueError(f"No process found for {capsule_id=} or {capsule_name=}")
        return None
    elif len(matched_process) > 1:
        raise ValueError(
            f"Multiple processes found for {capsule_id=} or {capsule_name=}"
        )
    return matched_process[0]


def get_capsule_url(capsule: Capsule) -> str:
    """Get the URL for a specific capsule.

    Args:
        capsule: Capsule object
    Returns:
        str: URL of the capsule
    """
    # github url is preferred, but its currently unreliable in duplicated capsules
    if capsule.cloned_from_url and capsule.original_capsule:
        return capsule.cloned_from_url

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
    processing: ps.Processing,
    s3_bucket: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Writes output and copies to s3.
    Process record is written to docdb

    Args:
        processing: Processing record
        s3_bucket: Bucket to copy results to

    """
    if s3_bucket is None:
        s3_bucket = os.getenv("ANALYSIS_BUCKET")
    metadata, docdb_id = create_results_metadata(processing, s3_bucket)
    with open("/results/metadata.nd.json", "w") as f:
        f.write(metadata.model_dump_json(indent=2))
    if not dry_run:
        copy_results_to_s3(metadata)
        write_to_docdb(metadata, docdb_id)
