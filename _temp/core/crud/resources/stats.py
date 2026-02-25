from typing import Dict, List

from pydantic import BaseModel, field_validator, ValidationError


class StatItem(BaseModel):
    pk: int = None
    timestamp: float
    tags: List[str]
    uid: str
    data: Dict
