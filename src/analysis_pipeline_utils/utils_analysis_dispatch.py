"""
Functions for analysis dispatcher
"""

import csv
import json
import logging
import uuid
from pathlib import Path
from requests.exceptions import HTTPError
from typing import Any, Iterator, List, Optional, Union

from s3fs import S3FileSystem

from analysis_pipeline_utils.analysis_dispatch_model import (
    AnalysisDispatchModel,
)
from analysis_pipeline_utils.metadata import (
    construct_processing_record,
    docdb_record_exists,
    get_codeocean_process_metadata,
    get_docdb_client,
)

logger = logging.getLogger(__name__)

API_GATEWAY_HOST = "api.allenneuraldynamics.org"
DATABASE = "metadata_index"
COLLECTION = "data_assets"

docdb_api_client = get_docdb_client(
    host=API_GATEWAY_HOST,
    database=DATABASE,
    collection=COLLECTION,
)
fs = S3FileSystem(use_listings_cache=False)


def query_data_assets(
    query: dict,
    group_by: Optional[List[str]] = None,
    filter_latest: Optional[str] = None,
    filter_by: Optional[str] = None,
    unwind_list_fields: Optional[List[str]] = None,
    drop_null_groups: bool = True,
    **unused_kwargs: Any,
) -> List[str]:
    """
    Query DocDB for data assets and build aggregation pipeline.

    Constructs and executes a MongoDB aggregation pipeline to retrieve and
    optionally group data asset records from DocDB.

    Parameters
    ----------
    query : dict
        A dictionary representing the query criteria used to filter data assets.

    group_by : Optional[List[str]], default None
        A list of DocDB record fields to group records by. If None, no grouping
        is performed and records are projected as-is.

    filter_latest : Optional[str], default None
        A DocDB record field to use to filter latest records. If provided,
        records will be sorted by this field in descending order, and only the
        most recent record in each group will be retained.

    filter_by : Optional[str], default None
        Fields to group by when filtering latest records. Required if
        ``filter_latest`` is provided.

    unwind_list_fields : Optional[List[str]], default None
        List of fields to unwind (flatten) before grouping. Useful for
        normalizing array-type fields.

    drop_null_groups : bool, default True
        If True, filter out records where grouping fields are None.

    Returns
    -------
    List[dict]
        A list of aggregation results. Each dict contains ``s3_location``,
        ``docdb_record_id``, and optionally ``group_metadata`` fields.

    Raises
    ------
    ValueError
        If ``filter_latest`` is provided without ``filter_by``.
    """
    pipeline = [{"$match": query}]

    all_group_fields = (filter_by or []) + (group_by or [])
    if drop_null_groups and len(all_group_fields) > 0:
        pipeline.append({"$match": {x: {"$ne": None} for x in all_group_fields}})

    if filter_latest:
        if not filter_by:
            raise ValueError("filter_by must be provided when filter_latest is used")
        pipeline.append({"$sort": {filter_latest: -1}})
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
        field_names = [field.replace(".", "__") for field in group_by]
        pipeline.append(
            {
                "$group": {
                    "_id": [f"${field}" for field in group_by],
                    "s3_location": {"$push": "$location"},
                    "docdb_record_id": {"$push": "$_id"},
                    **{
                        f"{field_names[i]}": {"$first": f"${field}"}
                        for i, field in enumerate(group_by)
                    },
                }
            }
        )
        # Nest metadata fields into group_metadata object
        pipeline.append(
            {
                "$addFields": {
                    "group_metadata": {field: f"${field}" for field in field_names}
                }
            }
        )
        # Remove flattened metadata fields
        pipeline.append(
            {
                "$project": {
                    **{f"{field}": 0 for field in field_names},
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
    logger.info(f"Aggregation pipeline: {pipeline}")
    try:
        response = docdb_api_client.aggregate_docdb_records(pipeline=pipeline)
    # print body of HTTP error
    except HTTPError as e:
        logger.error(f"Error aggregating DocDB records: {e}")
        if e.response is not None:
            logger.error(f"HTTP error response: {e.response.text}")
        raise

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
    Retrieve a list of data asset records from DocDB or CSV.

    Loads asset IDs from a CSV file or queries DocDB directly, then returns
    a list of AnalysisDispatchModel instances.

    Parameters
    ----------
    input_directory : Path
        Directory containing optional CSV file or query files.

    use_data_asset_csv : bool, default False
        If True, load asset IDs from the first CSV file found in
        ``input_directory`` and query DocDB for those assets.

    docdb_query : Union[str, Path, None], default None
        Either a path to a JSON file containing a DocDB query, or a JSON
        string representation of the query. Only used if
        ``use_data_asset_csv`` is False.

    **query_args
        Additional keyword arguments to pass to ``query_data_assets()``.
        Common examples: ``group_by``, ``filter_latest``, ``filter_by``.

    Returns
    -------
    List[AnalysisDispatchModel]
        A list of analysis dispatch models representing input data assets.

    Raises
    ------
    FileNotFoundError
        If ``use_data_asset_csv`` is True but no CSV file is found in
        ``input_directory``.
    ValueError
        If neither ``use_data_asset_csv`` nor ``docdb_query`` is provided.
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
    else:
        raise ValueError("No data asset input method specified")

    logger.info(f"Returned {len(records)} records")
    return [AnalysisDispatchModel(**record) for record in records]


def get_asset_file_path_records(
    record: AnalysisDispatchModel,
    file_extension: str,
    split_files: bool = False,
) -> List[AnalysisDispatchModel]:
    """
    Discover S3 files within data assets and return updated dispatch models.

    Searches each S3 location in the record for files matching the specified
    extension. Assets with no matching files are skipped entirely. If files
    are found, the record's ``file_location`` field is updated with full
    S3 paths (prefixed with ``s3://``).

    Parameters
    ----------
    record : AnalysisDispatchModel
        An analysis dispatch model representing input data assets.

    file_extension : str
        The file extension to search for (e.g., ``.tif``, ``.nwb``).

    split_files : bool, default False
        If True, return one model per matched file. If False, all matched
        files are collected into a single list in one model.

    Returns
    -------
    List[AnalysisDispatchModel]
        A list of models with updated ``file_location`` fields. Empty list
        if no files match the extension.

    Raises
    ------
    ValueError
        If ``split_files`` is True and the record contains multiple grouped
        assets (i.e., ``len(s3_location) > 1``).
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


def expand_task_list(
    input_records: List[AnalysisDispatchModel],
    file_extension: str = "",
    split_files: bool = True,
    distributed_analysis_parameters: Optional[List[dict[str, Any]]] = None,
) -> Iterator[AnalysisDispatchModel]:
    """
    Expand list of tasks to dispatch with input file details and parameter expansion.

    For each input record, optionally discovers matching S3 files and/or
    expands the model for each provided parameter set, resulting in one or
    more output models per input record.

    Parameters
    ----------
    input_records : List[AnalysisDispatchModel]
        A list of analysis dispatch models representing input data assets.

    file_extension : str, default ""
        File extension to search for within each asset (e.g., ``.tif``).
        If empty, no file discovery is performed.

    split_files : bool, default True
        If True and ``file_extension`` is provided, return one model per
        matched file. If False, group all files per asset in a single model.

    distributed_analysis_parameters : Optional[List[dict[str, Any]]], default None
        Optional list of parameter dictionaries. If provided, each record is
        expanded into multiple models, one per parameter set, with the
        ``distributed_parameters`` field populated accordingly.

    Returns
    -------
    Iterator[AnalysisDispatchModel]
        Expanded list of dispatch models. Cardinality is determined by the
        product of input records x file matches (if split) x parameter sets.
    """
    for record in input_records:
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
                    yield record.model_copy(update={"distributed_parameters": params})
            else:
                yield record


def check_task_parameters(
    input_model_list: Iterator[AnalysisDispatchModel],
    fixed_analysis_params: Optional[dict[str, Any]] = None,
    filter_processed: bool = True,
) -> Iterator[AnalysisDispatchModel]:
    """
    Filters out already processed analysis jobs from the input model list.

    Parameters
    ----------
    input_model_list : Iterator[AnalysisDispatchModel]
        An iterator of AnalysisDispatchModel instances to be filtered.

    Returns
    -------
    Iterator[AnalysisDispatchModel]
        An iterator of AnalysisDispatchModel instances that have not been processed yet.
    """
    if fixed_analysis_params is None:
        fixed_analysis_params = {}
    base_process = get_codeocean_process_metadata(from_dispatch=True)
    for model in input_model_list:
        process = construct_processing_record(
            base_process, model, **fixed_analysis_params
        )
        if filter_processed and docdb_record_exists(process):
            logger.info(
                f"Skipping already processed job for assets {model.docdb_record_id}"
            )
        else:
            model.analysis_code = process.code
            yield model


def write_input_model_list(
    input_model_list: Iterator[AnalysisDispatchModel],
    output_directory: Path,
    tasks_per_job: int = 1,
    max_number_of_tasks_dispatched: int = 1000,
) -> None:
    """
    Distributes a list of input models across a specified number of tasks per job,
    writes the models to disk in JSON format, and logs the progress.

    Parameters
    ----------
    input_model_list : list of AnalysisDispatchModel
        A list of AnalysisDispatchModel instances to be processed and written to disk.

    tasks_per_job: int = 1 : int
        The number of tasks to group per job when dispatching

    max_number_of_tasks_dispatched: int, Default = 1000
        The maximum number of tasks to dispatch

    Returns
    -------
    None
        This function does not return any value.
        It writes JSON files to disk for each input model.
    """

    if tasks_per_job < 1:
        raise ValueError("tasks_per_job must be at least 1")

    for task_id, task_model in enumerate(input_model_list):
        if task_id == max_number_of_tasks_dispatched:
            break
        job_id = task_id // tasks_per_job
        job_folder = output_directory / f"{job_id}"
        job_folder.mkdir(parents=True, exist_ok=True)

        with open(job_folder / f"{uuid.uuid4()}.json", "w") as f:
            f.write(task_model.model_dump_json(indent=4))
