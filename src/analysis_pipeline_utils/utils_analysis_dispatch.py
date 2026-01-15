"""
Functions for analysis dispatcher
"""

import csv
import json
import logging
from pathlib import Path
from typing import Any, List, Optional, Union

from s3fs import S3FileSystem

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
fs = S3FileSystem(anon=True, use_listings_cache=False)


def query_data_assets(
    query: dict,
    group_by: Optional[List[str]] = None,
    filter_obsolete: Optional[str] = None,
    filter_by: Optional[str] = None,
    unwind_list_fields: Optional[List[str]] = None,
    drop_null_groups: bool = True,
) -> List[str]:
    """
    Retrieve data asset paths and docdb record ids based on query passed in.

    Parameters
    ----------
    query : dict
        A dictionary representing the query criteria used to filter data assets

    group_by: Optional[List[str]], Default None
        A list of docdb record fields to use to group records into jobs.

    filter_obsolete: Optional[str], Default None
        A docdb record field to use to filter obsolete records.
        If provided, records will be sorted by this field in descending order,
        and only the most recent record in each group will be retained.

    Returns
    -------

    """
    pipeline = [{"$match": query}]

    if filter_obsolete:
        if not filter_by:
            raise ValueError(
                "filter_by must be provided when filter_obsolete is used"
            )
        pipeline.append({"$sort": {filter_obsolete: -1}})
        if drop_null_groups:
            pipeline.append({"$match": {x: {"$ne": None} for x in filter_by}})
        pipeline.append(
            {
                "$group": {
                    "_id": [f"${field}" for field in filter_by],
                    "record": {"$first": "$$ROOT"},
                }
            }
        )
        pipeline.append({"$replaceRoot": {"newRoot": "$record"}})
    if unwind_list_fields:
        for field in unwind_list_fields:
            pipeline.extend(
                [
                    {
                        "$addFields": {
                            field: {
                                "$cond": {
                                    "if": {"$isArray": f"${field}"},
                                    "then": f"${field}",
                                    "else": [f"${field}"],
                                }
                            }
                        }
                    },
                    {"$unwind": f"${field}"},
                ]
            )
    if group_by:
        if drop_null_groups:
            pipeline.append({"$match": {x: {"$ne": None} for x in group_by}})
        pipeline.append(
            {
                "$group": {
                    "_id": [f"${field}" for field in group_by],
                    "s3_location": {"$push": "$location"},
                    "docdb_record_id": {"$push": "$_id"},
                    "group_metadata": {
                        field: {"$first": f"${field}"} for field in group_by
                    },
                }
            }
        )
    else:
        pipeline.append(
            {
                "$project": {
                    "s3_location": ["$location"],
                    "docdb_record_id": ["$_id"],
                }
            }
        )
    response = docdb_api_client.aggregate_docdb_records(pipeline=pipeline)

    return response


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


def get_data_asset_records(
    input_directory: Path,
    use_data_asset_csv: bool = False,
    docdb_query: Union[str, Path, None] = None,
    **query_args,
) -> List[AnalysisDispatchModel]:
    """
    Retrieve a list of data asset paths and record ids
    based on the provided arguments.

    Parameters
    ----------
    use_data_asset_csv: bool, Default False
        Whether to use a user-provided csv with data asset ids

    docdb_query: Union[str, Path, None], Default None
        Path to json with query or json string representation

    Returns
    -------
    records: List[AnalysisDispatchModel]
        A list of analysis dispatch models representing input data assets.
    """
    if use_data_asset_csv:
        data_asset_ids_path = tuple(input_directory.glob("*.csv"))
        if not data_asset_ids_path:
            raise FileNotFoundError("Using data asset ids, but no path to csv provided")

        data_asset_ids = read_asset_ids_from_csv(data_asset_ids_path[0])
        records = query_data_assets(
            query={"external_links.Code Ocean.0": {"$in": data_asset_ids}},
            **query_args,
        )

    elif docdb_query:
        logger.info("Using query")
        if isinstance(docdb_query, str) and Path(docdb_query).exists():
            logger.info(f"Query input as json file at path {Path(docdb_query)}")
            with open(Path(docdb_query), "r") as f:
                query = json.load(f)
        else:
            query = json.loads(docdb_query)

        logger.info(f"Query {query}")
        records = query_data_assets(query=query, **query_args)
        query_str = json.dumps(query)
        for r in records:
            r["query"] = query_str

    logger.info(f"Returned {len(records)} records")
    return [AnalysisDispatchModel(**record) for record in records]


def get_asset_file_path_records(
    record: AnalysisDispatchModel,
    file_extension: str,
    split_files: bool = False,
) -> List[AnalysisDispatchModel]:
    """
    Returns tuple of list of s3 buckets, list of s3 paths,
    looking for the file extension if specified and list of
    docdb record ids for each asset

    If file extension is specified and is not found for a data asset,
    the record is skipped entirely and will not be part of the
    result list of assets

    Parameters
    ----------
    record : AnalysisDispatchModel
        An analysis dispatch model, representing input data assets.

    file_extension : str
        The file extension to search for in each data asset.

    split_files : bool
        Whether or not to split multiple matched files into separate models
        or to store in one model as a single list.

    Returns
    -------
    List[AnalysisDispatchModel]
        A list of analysis dispatch models with updated file locations.
    """
    s3_file_paths = []

    s3_paths_to_use = []
    docdb_ids_to_use = []
    data_asset_paths = record.s3_location
    docdb_record_ids = record.docdb_record_id
    if split_files and len(data_asset_paths) > 1:
        raise ValueError(
            "split_files not supported when processing grouped data assets"
        )
    for index, location in enumerate(data_asset_paths):
        file_paths = [f"s3://{f}" for f in fs.glob(f"{location}/**/*{file_extension}")]
        if not file_paths:
            logging.warning(f"No {file_extension} found in {location} - skipping.")
            continue

        s3_file_paths.extend(file_paths)
        logger.info(f"Found {len(file_paths)} *{file_extension} files from s3")

        # add records where file extension has been found
        s3_paths_to_use.append(location)
        docdb_ids_to_use.append(docdb_record_ids[index])
    if not s3_paths_to_use:
        return []
    if split_files:
        # single input asset, no need to update s3 location or docdb id
        return [
            record.model_copy(update={"file_location": [path]})
            for path in s3_file_paths
        ]
    return [
        AnalysisDispatchModel(
            s3_location=s3_paths_to_use,
            file_location=s3_file_paths,
            docdb_record_id=docdb_ids_to_use,
        )
    ]


def get_input_model_list(
    records: List[AnalysisDispatchModel],
    file_extension: str = "",
    split_files: bool = True,
    distributed_analysis_parameters: Optional[List[dict[str, Any]]] = None,
) -> List[AnalysisDispatchModel]:
    """
    Create analysis dispatch models from grouped data assets.

    Parameters
    ----------
    records : List[AnalysisDispatchModel]
        A list of analysis dispatch models representing input data assets.

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


    models: List[AnalysisDispatchModel] = []

    for record in records:
        if file_extension:
            file_records = get_asset_file_path_records(
                record,
                file_extension=file_extension,
                split_files=split_files,
            )
        else:
            file_records = [record]
        for record in file_records:
            if distributed_analysis_parameters:
                for params in distributed_analysis_parameters:
                    models.append(
                        record.model_copy(update={"distributed_parameters": params})
                    )
            else:
                models.append(record)
    return models
