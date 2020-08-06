from dataclasses import dataclass

from openapi.data.fields import uuid_field


@dataclass
class UUIdSchema:
    id: str = uuid_field(required=True, description="Unique ID")
