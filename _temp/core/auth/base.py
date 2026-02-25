from secrets import token_hex
from abc import abstractmethod
from typing import Optional, Tuple, Union, TypeVar

from django.conf import settings
from pydantic import BaseModel, ValidationError

from core.ecxeptions import AuthAPIError
from core.broker import AbstractMessageBroker
from core.repository import Repository
from core.crud.resources import User, Session, UserCredential
from core.utils import load_class


ERR_MESSAGE = str


SCHEMA_VALIDATOR = TypeVar('BaseModel')


class BaseAuthProvider:

    SecretsModel: SCHEMA_VALIDATOR
    CredModel: SCHEMA_VALIDATOR = None

    def __init__(self):
        self.__repo = Repository()

    @classmethod
    def get_engine_name(cls) -> str:
        return cls.__name__

    async def register_user(self, user_id: str, **extra) -> User:
        user = await self.__repo.get_user(user_id=user_id)
        if user:
            raise AuthAPIError(
                f'User with id: {user_id} already exists!'
            )
        user = await self.__repo.create_user(user_id, **extra)
        return user

    async def register_credentials(
        self, user_id: str, rewrite_exists: bool, **cred
    ) -> UserCredential:
        if self.CredModel:
            try:
                self.CredModel.model_validate(cred, strict=True)
            except ValidationError as e:
                raise AuthAPIError(e.errors())

        user = await self.__repo.get_user(user_id=user_id)
        if user is None:
            raise AuthAPIError(f'Not found user with id: {user_id}')
        _ = [
              cred for cred in user.credentials
              if cred.engine == self.get_engine_name()
            ]
        if len(_) > 0:
            exists_cred = _[0]
        else:
            exists_cred = None
        if exists_cred and not rewrite_exists:
            raise AuthAPIError(
                f'Credential {exists_cred.engine} '
                f'for user {user_id} already exists'
            )
        secrets = await self._create_secrets(**cred)
        if not secrets:
            raise AuthAPIError('Empty secrets')
        if exists_cred:
            await self.__repo.delete_credential(exists_cred)
        cred = await self.__repo.create_credential(
            user_id=user_id, cred_engine=self.get_engine_name(),
            secrets=secrets.model_dump(mode='json')
        )
        return cred

    async def login(
        self, user_id: str, **cred
    ) -> Tuple[Optional[Session], Optional[ERR_MESSAGE]]:
        if self.CredModel:
            try:
                self.CredModel.model_validate(cred, strict=True)
            except ValidationError:
                return None, 'Credentials has invalid format'
        broker = self._get_broker()
        user, credential = await self._get_credential(user_id)
        secrets = self.SecretsModel(**credential.secrets)
        success_or_sess_id, err_msg = await self._login(
            user, broker, secrets, **cred
        )
        if success_or_sess_id:
            if isinstance(success_or_sess_id, str):
                session_id = success_or_sess_id
            else:
                session_id = token_hex(16)
            session = await self.__repo.create_session(
                user_id=user_id,
                cred_engine=self.get_engine_name(),
                session_id=session_id
            )
            return session, None
        else:
            return None, err_msg

    async def authenticate(
        self, session_id: str, **cred
    ) -> Tuple[Optional[User], Optional[ERR_MESSAGE]]:
        if self.CredModel:
            try:
                self.CredModel.model_validate(cred, strict=True)
            except ValidationError:
                return None, 'Credentials has  invalid format'
        broker = self._get_broker()
        user, credential = await self._get_credential(session_id)
        secrets = self.SecretsModel(**credential.secrets)
        success, err_msg = await self._authenticate(
            broker, secrets, **cred
        )
        if success:
            return user, None
        else:
            return None, err_msg

    @abstractmethod
    async def _authenticate(
        self, broker: AbstractMessageBroker, secrets: 'SecretsModel', **cred
    ) -> Tuple[bool, Optional[ERR_MESSAGE]]:
        pass

    @abstractmethod
    async def _login(
        self, user: User, broker: AbstractMessageBroker,
        secrets: 'SecretsModel', **cred
    ) -> Tuple[Union[bool, str], Optional[ERR_MESSAGE]]:
        pass

    @abstractmethod
    async def _create_secrets(self, **cred) -> 'SecretsModel':
        pass

    @classmethod
    def _get_broker(cls) -> AbstractMessageBroker:
        broker_cls = load_class(settings.MESSAGE_BROKER['default'])
        if not issubclass(broker_cls, AbstractMessageBroker):
            raise AuthAPIError('Invalid message broker configuration')
        broker_cfg = settings.MESSAGE_BROKER.get('cfg', {})
        return broker_cls(**broker_cfg)

    async def _get_credential(self, user_id_or_session_id: str) -> Tuple[User, UserCredential]:
        user = await self.__repo.get_user(user_id=user_id_or_session_id)
        if user is None:
            user = await self.__repo.get_user(session_id=user_id_or_session_id)
        if user is None:
            raise AuthAPIError(
                f'Not found user with id: {user_id_or_session_id}'
            )
        _ = [
            cred for cred in user.credentials
            if cred.engine == self.get_engine_name()
        ]
        if not _:
            raise AuthAPIError(
                f'Empty credential data for engine: {self.get_engine_name()}'
            )
        else:
            credential = _[0]
        return user, credential
