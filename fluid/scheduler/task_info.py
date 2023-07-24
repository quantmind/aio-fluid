from pydantic import BaseModel, Field


class TaskInfo(BaseModel):
    name: str = Field(description="Task name")
    description: str = Field(description="Task description")
    priority: str = Field(description="Task priority")
    schedule: str | None = Field(default=None, description="Task schedule")
    enabled: bool = Field(default=True, description="Task enabled")
    last_run_end: int | None = Field(
        default=None, description="Task last run end as milliseconds since epoch"
    )
    last_run_duration: int | None = Field(
        default=None, description="Task last run duration in milliseconds"
    )
    last_run_state: str | None = Field(
        default=None, description="State of last task run"
    )
