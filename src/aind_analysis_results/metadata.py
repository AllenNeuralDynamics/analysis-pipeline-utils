"""
suggested process for analysis wrapper capsule:
    processing = construct_processing_record(user_input)
    if check_docdb_for_record(processing):
    ... run processing
    processing_complete = copy_results_to_s3(processing, s3_bucket)
    write_to_docdb(processing_complete)
"""

from datetime import datetime
import os
import subprocess
import aind_data_schema.core.processing as ps
from aind_data_access_api.document_db import MetadataDbClient
from codeocean import CodeOcean


def construct_processing_record(
    analysis_job_dict,  # say this contains nested analysis-specific metadata
    **kwargs,
) -> ps.DataProcess:
    process = query_code_ocean_metadata()
    # add s3_location and parameters from analysis_job_dict
    process.code.input_data.append(ps.DataAsset(url=analysis_job_dict["s3_location"]))
    process.code.parameters.update(analysis_job_dict["parameters"])
    # add any other metadata from user
    process.update(**kwargs)
    return process

def query_code_ocean_metadata():
    """
    Query Code Ocean API for metadata about the analysis
    """
    # Get the Code Ocean domain and API token from environment variables
    domain = os.getenv("CODEOCEAN_DOMAIN")
    token = os.getenv("CODEOCEAN_API_TOKEN")
    computation_id = os.getenv("CODEOCEAN_COMPUTATION_ID")
    
    if not all([domain, token, computation_id]):
        raise ValueError("Warning: Missing required Code Ocean environment variables")
    
    # Initialize the Code Ocean client
    client = CodeOcean(domain=domain, token=token)
    
    # Get the current computation details
    computation = client.computations.get_computation(computation_id)
    
    # Extract relevant metadata from the computation
    process = ps.DataProcess.model_construct(
        # name may be only set for runs named after the fact?
        # pull from capsule or pipeline name? 
        name=computation.name,
        process_type=ps.ProcessName.ANALYSIS,
        process_stage=ps.ProcessStage.ANALYSIS,
        start_date_time=datetime.fromtimestamp(computation.created),
        end_date_time=datetime.fromtimestamp(
            computation.created + computation.run_time
        ),
    )
    # git origin URL
    shell_command = "git remote get-url origin"
    result = subprocess.run(shell_command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        git_remote_url = result.stdout.strip()
    else:
        git_remote_url = ""
    # git commit hash
    shell_command = "git rev-parse HEAD"
    result = subprocess.run(shell_command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        git_commit_hash = result.stdout.strip()
    else:
        git_commit_hash = ""

    capsule = client.capsules.get_capsule(computation.capsule_id)
    # not sure if this is the best way to get this info
    # but not recorded in computation object?
    process.experimenters = [ps.Person(name=capsule.owner)]

    # pipeline-level ordered and named parameters
    code = ps.Code(
        # git remote URL from shell command
        url=git_remote_url,
        version=git_commit_hash,
        run_script="code/run",
        name=capsule.name,
    )
    parameters = {}
    if computation.parameters:
        parameters.update(
            {
                f"param_{i}": param
                for i, param in enumerate(computation.parameters)
                if param
            }
        )
    if computation.named_parameters:
        parameters.update(
            {
                param.param_name: param.value
                for param in computation.named_parameters
                if param
            }
        )
    # capsule-specific parameters
    if os.getenv("CO_CAPSULE_ID"):
        for process in computation.processes:
            if process.capsule_id == os.getenv("CO_CAPSULE_ID"):
                parameters.update(
                    {
                        param["name"] if param["name"] else f"param_{i}": param["value"]
                        for i, param in enumerate(process.parameters)
                        if param["value"]
                    }
                )
    code.parameters = parameters
    
    if computation.data_assets:
        code.input_data = [
            ps.DataAsset(get_data_asset_url(client, asset.id))
            for asset in computation.data_assets
        ]

    process.code = code
    return process
    


def get_data_asset_url(client, data_asset_id: str) -> str:
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
    response = client.insert_one_docdb_record(processing)
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
        filter_query={"code": processing.code},
    )
    if len(responses) == 1:
        return responses[0]
    elif len(responses) > 1:
        raise ValueError(
            "Multiple records found in document database. "
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
