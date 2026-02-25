from hashlib import md5
from typing import Tuple, Union, Optional

from pydantic import BaseModel

from core.broker import AbstractMessageBroker
from core.crud.resources import User
from .base import BaseAuthProvider, ERR_MESSAGE


class BasicAuthSecretSchema(BaseModel):
    password_hash: str


class BasicAuthCredSchema(BaseModel):
    password: str


class BasicAuthProvider(BaseAuthProvider):

    SecretsModel = BasicAuthSecretSchema
    CredModel = BasicAuthCredSchema

    async def _login(
        self, user: User, broker: AbstractMessageBroker,
        secrets: BasicAuthSecretSchema, **cred
    ) -> Tuple[Union[bool, str], Optional[ERR_MESSAGE]]:
        if self._auth(secrets, **cred):
            return True, None
        else:
            return False, 'Invalid credentials'

    async def _authenticate(
        self, broker: AbstractMessageBroker,
        secrets: BasicAuthSecretSchema, **cred
    ) -> Tuple[bool, Optional[ERR_MESSAGE]]:
        if self._auth(secrets, **cred):
            return True, None
        else:
            return False, 'Invalid credentials'

    async def _create_secrets(self, **cred) -> BasicAuthSecretSchema:
        credentials = BasicAuthCredSchema(**cred)
        secrets = BasicAuthSecretSchema(
            password_hash=md5(credentials.password.encode()).hexdigest()
        )
        return secrets

    @staticmethod
    def _auth(secrets: BasicAuthSecretSchema, **cred) -> bool:
        credential = BasicAuthCredSchema(**cred)
        h = md5(credential.password.encode()).hexdigest()
        return secrets.password_hash == h
