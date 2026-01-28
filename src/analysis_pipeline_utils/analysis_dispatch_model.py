"""
Class that represents the schema for analysis dispatch outputs
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from aind_data_schema.components.identifiers import Code

class AnalysisDispatchModel(BaseModel):
    """
    Represents the inputs passed to an analysis by the analysis dispatch
    """

    s3_location: List[str] = Field(
        ...,
        description="S3 bucket path of each asset to be analyzed.",
    )
    docdb_record_id: List[str] = Field(
        ...,
        description="DocDB record ids of each asset to be analyzed.",
    )
    file_location: Optional[List[str]] = Field(
        None,
        description="Full S3 path to specific file within each data asset.",
    )
    distributed_parameters: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Dictionary of analysis parameters distributed by the job dispatcher"
        ),
    )
    group_metadata: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "For grouped assets, the shared metadata fields that define the group."
        ),
    )
    query: Optional[str] = Field(
        None,
        description="The query used to retrieve the data assets from DocDB.",
    )
    analysis_code: Optional[Code] = Field(
        None,
        description="Metadata for the analysis code associated with this dispatch input.",
    )