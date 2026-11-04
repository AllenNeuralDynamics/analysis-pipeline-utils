"""Utility functions for creating quality control records from analysis outputs"""

from datetime import datetime
from typing import Dict, List, Optional

from aind_data_schema.base import GenericModel
from aind_data_schema.core.quality_control import (
    QCMetric,
    QCStatus,
    QualityControl,
    Stage,
    Status,
)

from analysis_pipeline_utils.metadata import get_modality_from_records


def output_parameters_to_quality_control(
    output_parameters: GenericModel,
    input_records: List[Dict],
    evaluator: str = "Automated",
    status: Status = Status.PASS,
    tags: Optional[Dict[str, str]] = None,
) -> QualityControl:
    """
    Map analysis output parameters to a QualityControl record,
    with one metric per output parameter.

    All metrics are given the same status.

    Parameters
    ----------
    output_parameters : GenericModel
        Output parameters model returned by the analysis
    input_records : List[Dict]
        Metadata records of the data assets the analysis was run on.
        The metric modality is the first modality listed in the records.
    evaluator : str, optional
        Name recorded as the evaluator of each metric's status,
        by default "Automated"
    status : Status, optional
        Status given to every metric, by default Status.PASS
    tags : Optional[Dict[str, str]], optional
        Tags applied to every metric, by default None

    Returns
    -------
    QualityControl
        Quality control record with a metric per output parameter

    Raises
    ------
    ValueError
        If none of the input records list a modality
    """
    modality = get_modality_from_records(input_records)
    timestamp = datetime.now().astimezone()
    # descriptions are only available for fields declared on the model,
    # extra fields allowed by GenericModel will not have one
    fields = type(output_parameters).model_fields

    metrics = [
        QCMetric(
            name=name,
            modality=modality,
            stage=Stage.ANALYSIS,
            value=value,
            status_history=[
                QCStatus(evaluator=evaluator, status=status, timestamp=timestamp)
            ],
            description=fields[name].description if name in fields else None,
            tags=tags or {},
        )
        for name, value in output_parameters.model_dump(mode="json").items()
    ]
    return QualityControl(
        metrics=metrics,
        default_grouping=list(tags) if tags else [],
    )
