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
import aind_data_schema.core.processing as ps
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.components.identifiers import DataAsset
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_schema.components.identifiers import DataAsset
from codeocean import CodeOcean
from codeocean.computation import Computation, PipelineProcess

PARAM_PREFIX = "param_"
CODEOCEAN_DOMAIN = os.getenv("CODEOCEAN_DOMAIN", "https://codecean.alleninstitute.org")


    
def extract_parameters(process: PipelineProcess | Computation) -> Dict[str, Any]:
    """Extract parameters for a specific capsule from computation processes.
    
    Args:
        process: PipelineProcess or Computation object containing parameters
        
    Returns:
        Dict[str, Any]: Parameters specific to the target capsule
    """
    return {
        param.name if param.name else f"{PARAM_PREFIX}{i}": param.value
        for i, param in enumerate(process.parameters)
        if param.value
    }

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
        
    process.code.parameters = process.code.parameters.model_copy(update=dict(
        **analysis_job_dict["parameters"], **kwargs
    ))

    return process

def _initialize_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client using environment variables.
    
    Returns:
        CodeOcean: Initialized Code Ocean client
        
    Raises:
        ValueError: If required environment variables are missing
    """
    domain = os.getenv("CODEOCEAN_DOMAIN")
    token = os.getenv("CODEOCEAN_API_TOKEN")
    
    if not all([domain, token]):
        raise ValueError("Warning: Missing required Code Ocean environment variables")
    
    return CodeOcean(domain=domain, token=token)

def query_code_ocean_metadata(capsule_id: Optional[str] = None) -> ps.DataProcess:
    """
    Query Code Ocean API for metadata about the current analysis pipeline or capsule run.

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
    pipeline = client.capsules.get_capsule(os.getenv("CO_PIPELINE_ID") or os.getenv("CO_CAPSULE_ID"))
    process.experimenters = [ps.Person(name=pipeline.owner)]
    process.name = pipeline.name

    parameters = extract_parameters(computation)
    # find the component process for this capsule or specified capsule ID
    capsule_id = capsule_id or os.getenv("CO_CAPSULE_ID")
    if not capsule_id:
        raise ValueError("Capsule ID must be provided or set in environment variable CO_CAPSULE_ID")
    component_process = next(proc for proc in computation.processes if proc.capsule_id == capsule_id)
    parameters.update(extract_parameters(component_process))
    capsule = client.capsules.get_capsule(capsule_id)
    code = ps.Code(
        url=capsule.cloned_from_url or "",
        name=capsule.name or "",
        version=str(component_process.version) or get_version_from_git_remote(capsule.slug),
        run_script="code/run",
        parameters=parameters,
        input_data = [
            DataAsset(get_data_asset_url(client, asset.id))
            for asset in computation.data_assets
        ]
    )

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

def get_version_from_git_remote(capsule_slug: str) -> str:
    """Get the git version for a specific capsule from the remote repository.

    Args:
        capsule_slug: Slug ID of the capsule to retrieve metadata for (7-digit numeric ID)

    Returns:
        str: Commit hash of the HEAD of the capsule's git repository
    """
    git_remote_url = f"\$CODEOCEAN_API_TOKEN@{CODEOCEAN_DOMAIN}/capsule-{capsule_slug}.git"
    git_commit_hash = _run_git_command(["git", "ls-remote", git_remote_url, "HEAD"])
    if not git_commit_hash:
        raise ValueError(f"Could not retrieve git commit hash for capsule {capsule_slug}")
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
        raise ValueError(f"Data asset source bucket {data_asset.source_bucket} not supported.")


def write_to_docdb(metadata: Metadata):
    """
    Write the processing record to the document database
    """
    client = get_docdb_client()
    response = client.insert_one_docdb_record(metadata.model_dump_json())
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
    
    responses = client.retrieve_docdb_records(
        filter_query={"processing.data_processes[0].code": processing.code.model_dump_json()},
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
