"""
Functions for analysis dispatcher
"""

import csv
import json
import logging
from pathlib import Path
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
) -> tuple[List[List[str]], List[List[str]]]:
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
    List[List[str]]
        A nested list of data asset paths
        that match the provided query criteria.

    List[List[str]]
        A nested list of docdb record ids that match the query

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


def read_asset_ids_from_csv(csv_path: Path) -> List[str]:
    """
    Read data asset IDs from a CSV file.

    The CSV file must contain a column named ``asset_id``. Rows with empty or
    missing values in this column are ignored. An error is raised if the column
    does not exist or if no valid asset IDs are found.

    Parameters
    ----------
    csv_path : pathlib.Path
        Path to the CSV file containing data asset IDs.

    Returns
    -------
    list of str
        List of non-empty data asset ID strings extracted from the CSV file.

    Raises
    ------
    ValueError
        If the CSV file does not contain an ``asset_id`` column or
        if the column exists but contains no valid (non-empty) values.
    """
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if "asset_id" not in reader.fieldnames:
            raise ValueError("CSV must contain an 'asset_id' column")

        asset_ids = [
            row["asset_id"].strip()
            for row in reader
            if row.get("asset_id") and row["asset_id"].strip()
        ]

    if not asset_ids:
        raise ValueError("Asset id column is empty")

    return asset_ids


def get_data_asset_paths_and_record_ids(
    input_directory: Path,
    use_data_asset_csv: bool = False,
    docdb_query: Union[str, Path, None] = None,
    group_by: Union[str, None] = None,
    **kwargs,
) -> tuple[list[str], list[str]]:
    """
    Retrieve a list of data asset paths and record ids
    based on the provided arguments.

    Parameters
    ----------
    use_data_asset_csv: bool, Default False
        Whether to use a user-provided csv with data asset ids

    docdb_query: Union[str, Path, None], Default None
        Path to json with query or json string representation

    group_by: Union[str, None], Default None
        Reference to a single docDB record field to use
        to group records into jobs. For example 'subject.subject_id

    Returns
    -------
    tuple[list of str, list of str]
        A list of data asset ID strings and record ids
          that match the provided filters.
    """
    if use_data_asset_csv:
        data_asset_ids_path = tuple(input_directory.glob("*.csv"))
        if not data_asset_ids_path:
            raise FileNotFoundError(
                "Using data asset ids, but no path to csv provided"
            )

        data_asset_ids = read_asset_ids_from_csv(data_asset_ids_path[0])
        data_asset_paths, docdb_ids = (
            get_data_asset_paths_and_docdb_id_from_query(
                query={"external_links.Code Ocean.0": {"$in": data_asset_ids}},
                group_by=group_by,
            )
        )

    elif docdb_query:
        logger.info("Using query")
        if isinstance(docdb_query, str) and Path(docdb_query).exists():
            logger.info(
                f"Query input as json file at path {Path(docdb_query)}"
            )
            with open(Path(docdb_query), "r") as f:
                query = json.load(f)
        else:
            query = json.loads(docdb_query)

        logger.info(f"Query {query}")
        data_asset_paths, docdb_ids = (
            get_data_asset_paths_and_docdb_id_from_query(query, group_by)
        )

    logger.info(f"Returned {len(data_asset_paths)} records")
    return data_asset_paths, docdb_ids


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

    If file extension is specified and is not found for a data asset,
    the record is skipped entirely and will not be part of the
    result list of assets

    Parameters
    ----------
    data_asset_paths: List[str]
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

    s3_file_paths: Union[List[str], List[List[str]]]
        A list of either single S3 file locations (URLs) that match the query
        and the specified file extension or a list of S3 file locations
        if multiple files are returned for the
        file extension and split_files is False.
        Each location is prefixed with "s3://".

    docdb_record_ids:
        A list of docdb record ids
    """
    s3_file_paths = []
    s3_file_system = s3fs.S3FileSystem()

    if file_extension == "":
        return data_asset_paths, s3_file_paths, docdb_record_ids

    # if file extension is specified and not found,
    # the record is skipped entirely,
    # need to maintain list of records to use

    s3_paths_to_use = []
    docdb_ids_to_use = []
    for index, location in enumerate(data_asset_paths):
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
        logger.info(f"Found {len(file_paths)} *{file_extension} files from s3")

        # add records where file extension has been found
        s3_paths_to_use.append(location)
        docdb_ids_to_use.append(docdb_record_ids[index])

    return s3_paths_to_use, s3_file_paths, docdb_ids_to_use


def get_input_model_list(
    data_asset_groups: List[List[str]],
    docdb_record_id_groups: List[List[str]],
    file_extension: str = "",
    split_files: bool = True,
    distributed_analysis_parameters: List[dict[str, Any]] | None = None,
) -> List[AnalysisDispatchModel]:
    """
    Create analysis dispatch models from grouped data assets.

    Parameters
    ----------
    data_asset_groups : List[List[str]]
        Grouped S3 data asset paths. Each inner list represents a single
        analysis group.

    docdb_record_id_groups : List[List[str]]
        Grouped DocDB record IDs aligned with ``data_asset_groups``.

    file_extension : str, optional
        File extension to search for within each group. If empty, no file
        discovery is performed.

    split_files : bool, optional
        Whether to split matching files into separate analysis inputs.

    distributed_analysis_parameters : List of dict, optional
        Optional analysis parameter dictionaries. If provided, a model is
        created for each parameter set per group.

    Returns
    -------
    list of AnalysisDispatchModel
        One or more dispatch models per input group.
    """

    assert len(data_asset_groups) == len(docdb_record_id_groups), (
        "data_asset_groups and docdb_record_id_groups "
        "must be the same length"
    )

    models: List[AnalysisDispatchModel] = []

    for asset_paths, record_ids in zip(
        data_asset_groups, docdb_record_id_groups
    ):
        s3_buckets, s3_paths, metadata_record_ids = (
            get_s3_and_docdb_input_information(
                data_asset_paths=asset_paths,
                docdb_record_ids=record_ids,
                file_extension=file_extension,
                split_files=split_files,
            )
        )

        if not s3_buckets:
            continue
        file_location = s3_paths if s3_paths else None

        if distributed_analysis_parameters:
            for params in distributed_analysis_parameters:
                models.append(
                    AnalysisDispatchModel(
                        s3_location=s3_buckets,
                        file_location=file_location,
                        docdb_record_id=metadata_record_ids,
                        distributed_parameters=params,
                    )
                )
        else:
            models.append(
                AnalysisDispatchModel(
                    s3_location=s3_buckets,
                    file_location=file_location,
                    docdb_record_id=metadata_record_ids,
                )
            )

    return models
