"""
Functions for analysis dispatcher
"""

import logging
from typing import Any, List, Optional, Union

import s3fs

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.metadata import get_docdb_client

logger = logging.getLogger(__name__)

API_GATEWAY_HOST = "api.allenneuraldynamics.org"
DATABASE = "metadata_index"
COLLECTION = "data_assets"

docdb_api_client = get_docdb_client(
    host=API_GATEWAY_HOST,
    database=DATABASE,
    collection=COLLECTION,
)


def get_data_asset_paths_and_docdb_id_from_query(
    query: dict, group_by: Optional[str]
) -> tuple[List[str], List[str]]:
    """
    Retrieve data asset paths and docdb record ids based on query passed in.

    Parameters
    ----------
    query : dict
        A dictionary representing the query criteria used to filter data assets

    group_by: str, Optional
        Field to group results from query by

    Returns
    -------
    List of str
        A list of data asset paths
        that match the provided query criteria.

    List of str
        A list of docdb record ids that match the query

    """

    asset_id_prefix = "location"
    response = docdb_api_client.aggregate_docdb_records(
        pipeline=[
            {"$match": query},
            {
                "$group": {
                    "_id": "$" + group_by if group_by else "$_id",
                    "asset_location": {"$push": f"${asset_id_prefix}"},
                    "docdb_id": {"$push": "$_id"},
                }
            },
        ]
    )

    locations = [x["asset_location"] for x in response]
    docdb_ids = [x["docdb_id"] for x in response]
    return locations, docdb_ids


def get_s3_and_docdb_input_information(
    data_asset_paths: List[str],
    docdb_record_ids: List[str],
    file_extension: str = "",
    split_files: bool = True,
) -> tuple[List[str], Union[List[str], List[List[str]]], List[str]]:
    """
    Returns tuple of list of s3 buckets, list of s3 paths,
    looking for the file extension if specified and list of
    docdb record ids for each asset

    Parameters
    ----------
    data_asset_paths: list[str]
        A list of paths to data assets in S3

    file_extension : str, optional
        The file extension to filter for when searching the S3 locations.
        If no file extension is provided,
        the path to the bucket is returned from the query

    split_files : bool
        Whether or not to split files into seperate models
        or to store in one model as a single list.

    Returns
    -------
    s3_paths: list of str
        A list of S3 bucket paths

    s3_file_paths: list of str
        A list of either single S3 file locations (URLs) that match the query
        and the specified file extension or a list of S3 file locations
        if multiple files are returned for the
        file extension and split_files is False.
        Each location is prefixed with "s3://".

    docdb_record_ids:
        A list of docdb record ifs
    """
    s3_paths = []
    s3_file_paths = []
    s3_file_system = s3fs.S3FileSystem()
    docdb_ids = []

    for index, location in enumerate(data_asset_paths):
        if file_extension != "":
            file_paths = tuple(
                s3_file_system.glob(f"{location}/**/*{file_extension}")
            )
            if not file_paths:
                logging.warning(
                    f"No {file_extension} found in {location} - skipping."
                )
                continue

            if split_files:
                for file in file_paths:
                    s3_file_paths.append(f"s3://{file}")
            else:
                s3_file_paths.append([f"s3://{file}" for file in file_paths])
            logger.info(
                f"Found {len(file_paths)} *{file_extension} files from s3"
            )

        s3_paths.append(location)
        docdb_ids.append(docdb_record_ids[index])

    return s3_paths, s3_file_paths, docdb_ids


def get_input_model_list(
    data_asset_paths: Union[List[str], List[List[str]]],
    docdb_record_ids: Union[List[str], List[List[str]]],
    file_extension: str = "",
    split_files: bool = True,
    distributed_analysis_parameters: Union[List[dict[str, Any]], None] = None,
) -> list[AnalysisDispatchModel]:
    """
    Writes the input model with the
    S3 location from the query and input arguments

    Parameters
    ----------

    data_asset_paths: Union[list[str], list[list[str]], None]
        The data asset paths to get input models for.
        Either a flat list or nested list of lists.

    docdb_record_ids: Union[list[str], list[list[str]], None]
        The docdb record ids to get input models for.
        Either a flat list or nested list of lists.

    file_extension : str, optional
        The file extension to filter for when searching the S3 locations.
        Defaults to empty, meaning the bucket path
        will be returned from the query.

    split_files : bool, optional
        Whether or not to split files into seperate models
        or to store in one model as a single list.

    distributed_analysis_parameters: Union[list[dict[str, Any]], None]
        List of dicts of analysis parameters.
        The dispatch will compute the product over
        input data and analysis dict for each in list.

    Returns
    -------
    list: AnalysisDispatchModel
        Returns a list of input analysis jobs
    """

    def make_models(
        s3_buckets: List[str], s3_paths: List[str], docdb_record_ids: List[str]
    ) -> List[AnalysisDispatchModel]:
        """
        Creates input model list

        Parameters
        ----------
        s3_buckets: List[str]
            The paths to s3 buckets

        s3_paths:
            The paths to files in s3 if file extension
            specified

        docdb_record_ids: List[str]
            The docdb ids to be used for querying

        Returns
        -------
        List: AnalysisDispatchModel
            The list of input models
        """
        models = []
        if is_flat:
            for index, s3_bucket in enumerate(s3_buckets):
                file_location = [s3_paths[index]] if s3_paths else None
                if distributed_analysis_parameters:
                    for parameters in distributed_analysis_parameters:
                        models.append(
                            AnalysisDispatchModel(
                                s3_location=[s3_bucket],
                                file_location=file_location,
                                distributed_parameters=parameters,
                                docdb_record_id=[docdb_record_ids[index]],
                            )
                        )
                else:
                    models.append(
                        AnalysisDispatchModel(
                            s3_location=[s3_bucket],
                            file_location=file_location,
                            docdb_record_id=[docdb_record_ids[index]],
                        )
                    )
        else:
            file_location = s3_paths if s3_paths else None
            if distributed_analysis_parameters:
                for parameters in distributed_analysis_parameters:
                    models.append(
                        AnalysisDispatchModel(
                            s3_location=s3_buckets,
                            file_location=file_location,
                            distributed_parameters=parameters,
                            docdb_record_id=docdb_record_ids,
                        )
                    )
            else:
                models.append(
                    AnalysisDispatchModel(
                        s3_location=s3_buckets,
                        file_location=file_location,
                        docdb_record_id=docdb_record_ids,
                    )
                )
        return models

    # Normalize to grouped format
    is_flat = isinstance(data_asset_paths, list) and all(
        isinstance(i, str) for i in data_asset_paths
    )

    grouped_assets = (
        [data_asset_paths]
        if is_flat
        else (
            data_asset_paths
            if all(isinstance(i, list) for i in data_asset_paths)
            else []
        )
    )

    grouped_docdb_record_ids = (
        [docdb_record_ids]
        if is_flat
        else (
            docdb_record_ids
            if all(isinstance(i, list) for i in docdb_record_ids)
            else []
        )
    )

    logger.info(
        "Flat data asset ids list provided"
        if is_flat
        else "Nested data asset ids list provided"
    )

    all_grouped_models = []
    for index, group in enumerate(grouped_assets):
        s3_buckets, s3_paths, asset_docdb_record_ids = (
            get_s3_and_docdb_input_information(
                data_asset_paths=group,
                docdb_record_ids=grouped_docdb_record_ids[index],
                file_extension=file_extension,
                split_files=split_files,
            )
        )
        if not s3_buckets:
            continue

        all_grouped_models.extend(
            make_models(s3_buckets, s3_paths, asset_docdb_record_ids)
        )

    return all_grouped_models
