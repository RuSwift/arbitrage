import logging
from secrets import token_hex
from typing import Optional, Tuple, List

from channels.db import database_sync_to_async as sync_to_async
from django.db import transaction
from django.db.models import Q

from core.crud.base import CRUD
from core.crud.resources import User, UserCredential, Session
from core.models import UserAccount as DBUserAccount, \
    UserAccountCredential as DBUserAccountCredential, Session as DBSession


class CRUDUserInterface(CRUD):

    async def create_user(
        self, user_id: str, **extra
    ) -> User:

        def _sync_call():
            DBUserAccount.objects.create_user(
                username=user_id, **extra
            )
        await sync_to_async(_sync_call)()
        user = await self.get_user(user_id)
        await self._on_created(user)
        return user

    async def delete_user(self, user_id: str) -> bool:

        def _sync_call():
            with transaction.atomic():
                rec = DBUserAccount.objects.filter(username=user_id).first()
                if rec:
                    rec.delete()

        user = await self.get_user(user_id)
        if user:
            await sync_to_async(_sync_call)()
            await self._on_deleted(user)
            return True
        else:
            return False

    async def modify_user(self, user: User) -> User:
        if user.credentials is not None:
            logging.warning(
                'You can modify credentials in separate methods only!'
            )
        if user.sessions is not None:
            logging.warning(
                'You can modify sessions in separate methods only!'
            )

        def _sync_call(update: User) -> DBUserAccount:
            dump = update.model_dump()
            upd_values = {key: val for key, val in dump.items() if key not in ['id', 'credentials', 'sessions']}
            DBUserAccount.objects.filter(
                username=update.id
            ).update(
                **upd_values
            )
            upd_rec = DBUserAccount.objects.filter(username=update.id).first()
            return upd_rec

        await sync_to_async(_sync_call)(user)
        await self._on_modified(user)
        return await self.get_user(user.id)

    async def create_credential(
        self, user_id: str, cred_engine: str, secrets: dict
    ) -> UserCredential:

        def _sync_call(
            user_id_: str, obj_: UserCredential
        ) -> (DBUserAccount, DBUserAccountCredential):
            account = DBUserAccount.objects.filter(username=user_id_).first()
            if account is None:
                raise RuntimeError(
                    f'Unknown account with user_id: {user_id_}'
                )
            cred = DBUserAccountCredential.objects.create(
                engine=obj_.engine,
                account=account,
                secrets=obj_.secrets
            )
            return account, cred

        obj = UserCredential(
            pk=0,
            engine=cred_engine,
            secrets=secrets
        )
        rec_acc: DBUserAccount
        rec_cred: DBUserAccountCredential
        rec_acc, rec_cred = await sync_to_async(
            _sync_call
        )(user_id, obj)
        user_ = User(
            id=rec_acc.username,
            email=rec_acc.email
        )
        obj.pk = rec_cred.pk
        await self._on_created(target=obj)
        await self._on_modified(target=user_)
        return obj

    async def modify_credential(
        self, cred: UserCredential, clear_sessions: bool = False
    ) -> UserCredential:

        def _sync_call(
            cred_: UserCredential, clear_sessions_: bool
        ) -> (DBUserAccount, DBUserAccountCredential):
            with transaction.atomic():
                rec_cred_ = DBUserAccountCredential.objects.filter(
                    pk=cred_.pk
                ).first()
                if rec_cred_ is None:
                    raise RuntimeError(
                        f'Not found cred pk: {cred_.pk}'
                    )
                rec_cred_.secrets = cred.secrets
                rec_cred_.save()
                if clear_sessions_:
                    for sess in rec_cred_.sessions.all():
                        sess.delete()
                return rec_cred_.account, rec_cred_

        rec_account, rec_cred = await sync_to_async(
            _sync_call
        )(cred, clear_sessions)
        cred = UserCredential(
            pk=rec_cred.pk,
            engine=rec_cred.engine,
            secrets=rec_cred.secrets
        )
        await self._on_modified(
            target=[cred, User(id=rec_account.username)]
        )
        return cred

    async def delete_credential(self, cred: UserCredential):
        def _sync_call(cred_: UserCredential) -> Optional[DBUserAccount]:
            rec_cred_ = DBUserAccountCredential.objects.filter(
                pk=cred_.pk,
            ).first()
            if rec_cred_:
                acc = rec_cred_.account
                rec_cred_.delete()
                return acc
            else:
                return None

        rec_account = await sync_to_async(_sync_call)(cred)
        if rec_account:
            await self._on_modified(target=User(id=rec_account.username))

    async def create_session(
        self, user_id: str, cred_engine: str, session_id: str = None
    ) -> Session:

        def _sync_call() -> (DBUserAccount, DBSession, DBUserAccountCredential):
            with transaction.atomic():
                rec_account_ = DBUserAccount.objects.filter(
                    username=user_id
                ).first()
                if not rec_account_:
                    raise RuntimeError(
                        f'Not found user {user_id}'
                    )
                rec_cred_ = DBUserAccountCredential.objects.filter(
                    account=rec_account_, engine=cred_engine
                ).first()
                if not rec_cred_:
                    raise RuntimeError(
                        f'Not found cred {cred_engine} for user {user_id}'
                    )
                rec_session_ = DBSession.objects.create(
                    session_id=session_id or token_hex(12),
                    account=rec_account_,
                    credential=rec_cred_,
                    storage={}
                )
            return rec_account_, rec_session_, rec_cred_

        rec_account, rec_session, rec_cred = await sync_to_async(_sync_call)()
        session = Session(
            id=rec_session.session_id,
            storage=rec_session.storage,
            credential=UserCredential(
                pk=rec_cred.pk,
                engine=rec_cred.engine,
                secrets=rec_cred.secrets
            )
        )
        await self._on_modified(target=User(id=rec_account.username))
        await self._on_created(target=session)
        return session

    async def delete_session(self, session: Session):

        def _sync_call(session_id: str) -> Optional[DBUserAccount]:
            rec_session_ = DBSession.objects.filter(
                session_id=session_id
            ).first()
            if rec_session_:
                rec_session_.delete()
                return rec_session_.account
            else:
                return None

        rec_account = await sync_to_async(_sync_call)(session.id)
        if rec_account:
            await self._on_modified(target=User(id=rec_account.username))

    async def modify_session(self, session: Session):

        def _sync_call() -> DBSession:
            rec_sess_ = DBSession.objects.filter(
                session_id=session.id
            ).first()
            if not rec_sess_:
                raise RuntimeError(
                    f'Not found session: {session.id}'
                )
            rec_sess_.storage = session.storage
            rec_sess_.save()
            return rec_sess_

        await sync_to_async(_sync_call)()
        await self._on_modified(session)

    async def is_session_exists(self, session_id: str) -> bool:
        def _sync_call() -> bool:
            return DBSession.objects.filter(session_id=session_id).exists()
        return await sync_to_async(_sync_call)()

    async def get_session(self, session_id) -> Tuple[str, Session]:

        def _sync_call() -> (str, DBSession):
            rec_sess = DBSession.objects.filter(
                session_id=session_id
            ).first()
            if not rec_sess:
                raise RuntimeError(
                    f'Not fount session: {session_id}'
                )
            _session = Session(
                id=rec_sess.session_id, storage=rec_sess.storage,
                credential=UserCredential(
                    pk=rec_sess.credential.pk,
                    engine=rec_sess.credential.engine,
                    secrets=rec_sess.credential.secrets
                )
            )
            return rec_sess.account.user_id, _session

        user_id, session = await sync_to_async(_sync_call)()
        return user_id, session

    async def get_user(
        self, user_id: str = None, session_id: str = None
    ) -> Optional[User]:

        if user_id is None and session_id is None:
            raise RuntimeError('You must specify user_id or session_id')

        def _sync_call() -> Optional[User]:
            if user_id:
                rec_user = DBUserAccount.objects.filter(username=user_id).first()
            else:
                session = DBSession.objects.filter(session_id=session_id).first()
                if session:
                    rec_user = session.account
                else:
                    rec_user = None
            if rec_user:
                rec_sessions = list(
                    DBSession.objects.filter(account=rec_user).all()
                )
                rec_credentials = list(
                    DBUserAccountCredential.objects.filter(
                        account=rec_user
                    ).all()
                )
                credentials = [
                    UserCredential(
                        pk=rec.pk,
                        engine=rec.engine,
                        secrets=rec.secrets
                    )
                    for rec in rec_credentials
                ]
                credentials_map = {cred.pk: cred for cred in credentials}
                sessions = [
                    Session(
                        id=rec.session_id,
                        credential=credentials_map.get(rec.credential.pk),
                        storage=rec.storage
                    )
                    for rec in rec_sessions
                ]
                return User(
                    id=rec_user.user_id,
                    email=rec_user.email,
                    is_admin=rec_user.is_admin,
                    sessions=sessions,
                    credentials=credentials,
                    first_name=rec_user.first_name,
                    last_name=rec_user.last_name
                )
            else:
                return None

        return await sync_to_async(_sync_call)()

    async def list_users(self, pattern: str) -> List[User]:

        def _sync_call() -> List[str]:
            queryset = DBUserAccount.objects.filter(
                Q(username__iregex=pattern) | Q(email__iregex=pattern) |
                Q(first_name__iregex=pattern) | Q(last_name__iregex=pattern)
            )
            return [account.user_id for account in queryset.all()]

        user_ids = await sync_to_async(_sync_call)()
        users = []
        for user_id in user_ids:
            user = await self.get_user(user_id)
            users.append(user)
        return users