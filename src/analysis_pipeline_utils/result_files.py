import hashlib
from typing import Dict

import aind_data_schema.core.processing as ps
import fsspec
from aind_data_schema.core.metadata import Metadata


def list_results_files(metadata: Dict | Metadata) -> list[str]:
    """
    List all results files for a given processing job in an S3 bucket.

    Args:
        metadata: Metadata object or dictionary containing the analysis record

    Returns:
        List of result file paths on s3.
    """
    if not isinstance(metadata, dict):
        metadata = metadata.model_dump()
    s3_url = metadata["location"]
    fs = fsspec.filesystem("s3")
    return fs.glob(f"{s3_url}/**/*")


def copy_results_to_s3(metadata: Metadata, results_path="/results"):
    """
    Copy results from a local path to an S3 bucket.
    Args:
        metadata: Metadata object containing the S3 location.
        results_path: The local path to the results to be copied.
    """
    fs = fsspec.filesystem("s3")
    s3_url = metadata.location

    if fs.exists(s3_url):
        raise Exception(f"S3 path {s3_url} already exists.")

    fs.put(results_path, s3_url, recursive=True)


def create_results_metadata(
    process: ps.DataProcess, s3_bucket: str
) -> Metadata:
    """
    Create metadata for the results of a processing job.

    Args:
        process: The processing job to create metadata for.
        s3_bucket: The S3 bucket to store results in.

    Returns:
        Metadata: The created metadata object.
    """
    s3_prefix = _processing_prefix(process)
    s3_url = f"s3://{s3_bucket}/{s3_prefix}"

    md = Metadata(
        processing=ps.Processing.create_with_sequential_process_graph(
            data_processes=[process]
        ),
        # TODO: name matching prefix or something else?
        name=s3_prefix,
        location=s3_url,
    )
    return md


def _processing_prefix(process: ps.DataProcess) -> str:
    """
    Generate a unique ID for the processing based on its metadata.
    This includes a timestamp, so it will change with each run.
    """
    # TODO: should this ignore some fields? use process.code only?
    process_metadata = process.model_dump_json().encode("utf-8")

    return hashlib.sha256(process_metadata).hexdigest()
