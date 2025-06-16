"""
suggested process for analysis wrapper capsule:
    processing = construct_processing_record(user_input)
    if check_docdb_for_record(processing):
    ... run processing
    processing_complete = copy_results_to_s3(processing, s3_bucket)
    write_to_docdb(processing_complete)
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
import os
import subprocess
import uuid
import aind_data_schema.core.processing as ps
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_schema.components.identifiers import DataAsset
from codeocean import CodeOcean
from codeocean.computation import Computation, PipelineProcess

PARAM_PREFIX = "param_"

def extract_parameters(
    computation: Computation,
    capsule_id: Optional[str] = None
) -> Dict[str, Any]:
    """Extract and combine parameters from various sources in the computation.
    
    Args:
        computation: The computation object containing parameters
        capsule_id: Optional capsule ID to filter process-specific parameters
    
    Returns:
        Dict[str, Any]: Combined parameters from all sources
    """
    parameters = {}
    
    # Extract ordered parameters
    if computation.parameters:
        ordered_params = {
            f"{PARAM_PREFIX}{i}": param 
            for i, param in enumerate(computation.parameters)
            if param
        }
        parameters.update(ordered_params)
    """
    # Extract named parameters
    if computation.named_parameters:
        named_params = {
            param.param_name: param.value
            for param in computation.named_parameters
            if param
        }
        parameters.update(named_params)
    """
    # Extract capsule-specific parameters if capsule ID provided
    if capsule_id:
        process_params = _get_capsule_parameters(computation.processes, capsule_id)
        parameters.update(process_params)
    
    return parameters

def _get_capsule_parameters(
    processes: List[PipelineProcess],
    capsule_id: str
) -> Dict[str, Any]:
    """Extract parameters for a specific capsule from computation processes.
    
    Args:
        processes: List of computation processes
        capsule_id: Target capsule ID to filter parameters
        
    Returns:
        Dict[str, Any]: Parameters specific to the target capsule
    """
    if processes is None:
        return {}
    for process in processes:
        if process.capsule_id == capsule_id:
            return {
                param["name"] if param["name"] else f"{PARAM_PREFIX}{i}": param["value"]
                for i, param in enumerate(process.parameters)
                if param["value"]
            }
    return {}

def construct_processing_record(
    analysis_job_dict: Dict[str, Any],
    **kwargs,
) -> ps.DataProcess:
    """Construct a processing record by combining Code Ocean metadata with analysis job data.
    
    Args:
        analysis_job_dict: Dictionary containing analysis metadata including:
            - s3_location (str): S3 URL for input data
            - parameters (Dict[str, Any]): Analysis parameters to update
        **kwargs: Additional metadata to update on the process
        
    Returns:
        ps.DataProcess: Constructed processing record with combined metadata
    """
    process = query_code_ocean_metadata()
    # add s3_location and parameters from analysis_job_dict
    if process.code.input_data is None:
        process.code.input_data = [DataAsset(url=analysis_job_dict["s3_location"])]
    else:
        process.code.input_data.append(DataAsset(url=analysis_job_dict["s3_location"]))
        
    process.code.parameters.update(analysis_job_dict["parameters"])
    # add any other metadata from user
    process.code.parameters.update(**kwargs)
    return process

def query_code_ocean_metadata():
    """
    Query Code Ocean API for metadata about the analysis
    """
    # Get the Code Ocean domain and API token from environment variables
    domain = os.getenv("CODEOCEAN_DOMAIN")
    token = os.getenv("CODEOCEAN_API_TOKEN")
    computation_id = os.getenv("CO_COMPUTATION_ID")
    
    if not all([domain, token, computation_id]):
        raise ValueError("Warning: Missing required Code Ocean environment variables")
    
    # Initialize the Code Ocean client
    client = CodeOcean(domain=domain, token=token)
    
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
    pipeline = client.capsules.get_capsule(os.getenv("CO_CAPSULE_ID"))
    process.experimenters = [ps.Person(name=pipeline.owner)]
    process.name = pipeline.name

    # Extract parameters using the new helper function
    parameters = extract_parameters(
        computation,
        capsule_id=os.getenv("CO_CAPSULE_ID")
    )
    code = get_code_metadata_from_git()
    code.parameters = parameters
    
    if computation.data_assets:
        code.input_data = [
            DataAsset(get_data_asset_url(client, asset.id))
            for asset in computation.data_assets
        ]

    process.code = code
    return process

def _run_git_command(command: List[str], default: str = "") -> str:
    """Run a git command safely and return its output.
    
    Args:
        command: List of command arguments
        default: Default value to return if command fails
        
    Returns:
        str: Command output or default value if command fails
    """
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False  # Don't raise on non-zero exit
        )
        return result.stdout.strip() if result.returncode == 0 else default
    except subprocess.SubprocessError:
        return default

def get_code_metadata_from_git() -> ps.Code:
    """Get git metadata for the current repository.
    
    Returns:
        ps.Code: Code object with git metadata
    """
    # Use safer command list format instead of shell=True
    git_remote_url = _run_git_command(["git", "remote", "get-url", "origin"])
    git_commit_hash = _run_git_command(["git", "rev-parse", "HEAD"])
    
    # Get repo name from remote URL instead of shell command
    repo_name = git_remote_url.split("/")[-1].replace(".git", "") if git_remote_url else ""

    code = ps.Code(
        # git remote URL from shell command
        url=git_remote_url,
        version=git_commit_hash,
        run_script="code/run",
        name=repo_name,
    )
    return code


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
    bucket = client.data_assets.get_data_asset(data_asset_id).source_bucket
    if bucket.origin == "aws":
        return f"s3://{bucket.bucket}/{bucket.prefix}"
    else:
        raise ValueError(f"Data asset origin {bucket.origin} not supported")


def write_to_docdb(processing: ps.DataProcess):
    """
    Write the processing record to the document database
    """
    client = get_docdb_client()
    processing_dict = processing.model_dump()
    processing_dict["code"]["run_script"] = processing_dict["code"]["run_script"].as_posix()

    response = client.insert_one_docdb_record(processing_dict)
    return response


def docdb_record_exists(processing: ps.DataProcess):
    """
    Check the document database for whether a record already exists matching the analysis metadata
    """
    if get_docdb_record(processing):
        return True
    return False


def get_docdb_record(processing: ps.DataProcess):
    """
    Get the document database record for the given processing object
    """
    client: MetadataDbClient = get_docdb_client()
    processing_code_dict = processing.code.model_dump()
    processing_code_dict["run_script"] = processing_code_dict["run_script"].as_posix()
    
    responses = client.retrieve_docdb_records(
        filter_query={"code": processing_code_dict},
    )
    if len(responses) == 1:
        return responses[0]
    elif len(responses) > 1:
        raise ValueError(
            "Multiple records found in document database. This indicates a potential data integrity issue."
        )
    else:
        return None


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
