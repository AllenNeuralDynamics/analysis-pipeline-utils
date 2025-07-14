"""
Class that represents the schema for analysis dispatch
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AnalysisDispatchModel(BaseModel):
    """
    Represents the inputs passed to an analysis by the analysis dispatch
    """

    s3_location: List[str] = Field(
        ...,
        title="s3 location",
        description="S3 bucket path(s) used in analysis",
    )

    asset_id: List[str] = Field(
        ...,
        title="asset ID",
        description="Asset ID(s) associated with the bucket",
    )

    asset_name: List[str] = Field(
        ..., title="asset name", description="Name(s) of the data asset(s)"
    )

    file_location: Optional[List[str]] = Field(
        None,
        title="file location",
        description="Full s3 path to specific file within the data asset.",
    )

    distributed_parameters: Optional[Dict[str, Any]] = Field(
        None,
        title="Distributed parameters",
        description="Dictionary of parameters from distributed list"
    )
