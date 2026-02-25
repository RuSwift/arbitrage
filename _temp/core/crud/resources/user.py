import re
from typing import Dict, List

from pydantic import BaseModel, field_validator, ValidationError

email_regex = re.compile(
    r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+'
)


class UserCredential(BaseModel):
    pk: int
    # JWT, BasicAuth, etc
    engine: str
    # secrets - hashed or encrypted
    secrets: Dict


class Session(BaseModel):
    id: str
    credential: UserCredential
    storage: dict = None


class User(BaseModel):
    id: str
    email: str = None
    credentials: List[UserCredential] = None
    sessions: List[Session] = None
    is_admin: bool = False
    first_name: str = None
    last_name: str = None

    @field_validator("email")
    def validate_email(cls, value):
        if value:
            if not re.fullmatch(email_regex, value):
                raise ValueError("Invalid email")
        return value
