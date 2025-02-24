# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Globals(BaseModel):
    num_records: Annotated[Optional[int], Field(title="Num Records")] = 100
    model_suite: Annotated[Optional[str], Field(title="Model Suite")] = "apache-2.0"


class Step(BaseModel):
    name: Annotated[str, Field(title="Name")]
    task: Annotated[str, Field(title="Task")]
    inputs: Annotated[Optional[List[str]], Field(title="Inputs")] = None
    config: Annotated[Dict[str, Any], Field(title="Config")]


class Workflow(BaseModel):
    name: Annotated[str, Field(title="Name")]
    version: Annotated[Optional[str], Field(title="Version")] = "2"
    inputs: Annotated[Optional[Dict[str, Any]], Field(title="Inputs")] = None
    globals: Optional[Globals] = None
    steps: Annotated[Optional[List[Step]], Field(title="Steps")] = None
