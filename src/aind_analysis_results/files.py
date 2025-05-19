
import aind_data_schema.core.processing as ps

def copy_results_to_s3(process: ps.DataProcess, s3_bucket: str, results_path="/results"):
    """
    Copy results to s3, calculating the prefix from the metadata record.
    Returns metadata record updated with s3 storage location
    """
    s3_prefix = hash(process)
    s3_url = f"s3://{s3_bucket}/{s3_prefix}"
    # TODO - check if s3_url already exists
    # TODO - write to s3

    # TODO - this is now a relative path in schema, where do we put URL?
    process.output_path = s3_url

    # TODO - does metadata include Processing and DataDescription
    return process

