

import hashlib
import json

import aind_data_schema.core.processing as ps
import fsspec


def copy_results_to_s3(process: ps.DataProcess, s3_bucket: str, results_path="/results"):
    """
    Copy results to s3, calculating the prefix from the metadata record.
    Returns metadata record updated with s3 storage location.
    """
    fs = fsspec.filesystem('s3')
    s3_prefix = _processing_prefix(process)
    s3_url = f"s3://{s3_bucket}/{s3_prefix}"
    
    if fs.exists(s3_url):
        raise Exception(f"S3 path {s3_url} already exists.")

    fs.put(results_path, s3_url, recursive=True)
        
    process.output_path = s3_url

    # TODO - should metadata include Processing and DataDescription? DataProcess.output_path is supposed to be relative
    return process


def _processing_prefix(process: ps.DataProcess) -> str:
    """
    Generate a unique ID for the processing based on its metadata.
    """

    # Convert the process metadata to a JSON string
    process_metadata = str(process).encode('utf-8')
    
    # Create a hash of the metadata
    return hashlib.sha256(process_metadata).hexdigest()