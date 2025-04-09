"""
suggested process for analysis wrapper capsule:
    processing = construct_processing_record(user_input)
    if check_docdb_for_record(processing):
    ... run processing
    processing_complete = copy_results_to_s3(processing, s3_bucket)
    write_to_docdb(processing_complete)
"""

from aind_data_schema.core import Processing

def construct_processing_record(
        input_analysis_model, #say this contains nested analysis-specific metadata
        user_input, #fields like user name that Code Ocean doesn't know
        code_ocean_metadata=None, #fields pulled from CO API or optionally provided
) -> Processing:
    if code_ocean_metadata is None:
        code_ocean_metadata = query_code_ocean_metadata()
    # compile different sources of metadata into a single Processing record
    return processing

def query_code_ocean_metadata():
    """
    Query Code Ocean API for metadata about the analysis
    """
    # details - what metadata is available from the API?
    return code_ocean_metadata

def write_to_docdb(processing):
    """
    Write the processing record to the document database
    """
    # details - what is the docdb API?
    return docdb_response

def check_docdb_for_record(processing):
    """
    Check the document database for whether a record already exists matching the analysis metadata
    """
    # details - what is the docdb API?
    return docdb_response