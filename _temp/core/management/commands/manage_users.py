import logging
import re
import json
import asyncio
from enum import Enum
from contextlib import contextmanager

from django.conf import settings
from django.core.management.base import BaseCommand
from pydantic import ValidationError

from core.utils import load_class
from core.repository import Repository
from core.crud.resources.user import User as UserModel
from core.auth import BaseAuthProvider


CUR_TAB: int = 0
END = 'end'
USER_EXCLUDE_ATTRS = ['id', 'credentials', 'sessions']


class Cmd(tuple, Enum):
    LIST = ('list', 'List/Search all users')
    PROVIDERS = ('providers', 'List available auth providers')
    REGISTER = ('register', 'Register new user')
    EDIT = ('edit', 'Edit user data, credentials, sessions')
    EXIT = ('exit', 'Exit')


class EditSubCmd(tuple, Enum):
    INFO = ('user:info', 'Edit info')
    REM_CRED = ('cred:remove', 'Remove credential')
    CREATE_CRED = ('cred:create', 'Create/Rewrite credential')
    CLEAR_SESSIONS = ('session:clear', 'Clear all active sessions')


def pretty_print(msg: str, tabs: int = None):
    if tabs is None:
        tabs = CUR_TAB
    prefix = '\t' * tabs
    print(prefix + msg)


def pretty_print_json(obj, tabs: int = None, suffix: str = ''):
    js = json.dumps(obj)
    new_js = re.sub(
        '\n +',
        lambda match: '\n' + '\t' * (len(match.group().strip('\n'))),
        js
    )
    pretty_print(new_js + suffix, tabs)


def pretty_print_user(user: UserModel, tabs: int = None):
    js = user.model_dump(
        mode='json',
        exclude={'credentials', 'sessions'}
    )
    cred_list = [cred.engine for cred in user.credentials]
    suffix = ' / Cred=' + json.dumps(
        cred_list) + f' Sessions={len(user.sessions)}'
    pretty_print_json(js, suffix=suffix, tabs=tabs)


def pretty_input(prompt: str, tabs: int = None):
    if tabs is None:
        tabs = CUR_TAB
    prefix = '\t' * tabs
    i = input(prefix + '> ' + prompt)
    return i


@contextmanager
def input_block(hello: str = None):
    if hello:
        pretty_print(hello)
    global CUR_TAB
    CUR_TAB += 1
    try:
        yield
    finally:
        CUR_TAB -= 1


class Command(BaseCommand):

    """Manage user accounts"""

    def handle(self, *args, **options):
        asyncio.run(self.run())

    async def run(self):
        while True:
            cmd = self.read_input_cmd()
            await self.run_choice(cmd)

    async def run_choice(self, choice: str):
        if choice == Cmd.EXIT[0]:
            exit()
        repo = Repository()
        if choice == Cmd.LIST[0]:
            with input_block('Users:'):
                while True:
                    pattern = pretty_input('Pattern for search: ')
                    try:
                        users = await repo.list_users(pattern)
                    except Exception as e:
                        if e.args:
                            pretty_print('Error: ' + str(e.args[0]))
                        else:
                            pretty_print(str(e))
                    if users:
                        with input_block('loaded users...'):
                            for user in users:
                                pretty_print_user(user)
                    else:
                        pretty_print('! Empty users list for pattern')
                    choice = pretty_input(f'[{END}]/... - end or continue: ')
                    if choice == END:
                        break
        elif choice == Cmd.PROVIDERS[0]:
            with input_block('Auth providers:'):
                for i, prov in enumerate(settings.AUTH_PROVIDERS):
                    name = prov.split('.')[-1]
                    pretty_input(f'[{i}] - {name}')
        elif choice == Cmd.REGISTER[0]:
            await self.register_user(repo)
        elif choice == Cmd.EDIT[0]:
            await self.edit_user(repo)

    @staticmethod
    def read_input_cmd():
        with input_block('Available commands:'):
            for cmd, comment in list(Cmd):
                pretty_print(f'[{cmd}] - {comment}')
            avail_commands = [var[0] for var in list(Cmd)]
            while True:
                choice = pretty_input('Cmd: ')
                if choice not in avail_commands:
                    pretty_print(
                        f'Invalid choice, available values: '
                        f'[{", ".join(avail_commands)}]'
                    )
                else:
                    return choice

    async def register_user(self, repo: Repository):
        with input_block('Fill user attributes:'):
            while True:
                user_id = pretty_input('user id: ')
                u = await repo.get_user(user_id)
                if u:
                    pretty_print(
                        f'Error: user with id "{user_id}" already exists'
                    )
                    continue
                else:
                    attrs = UserModel.model_fields
                    while True:
                        values = {}
                        for name, meta in attrs.items():
                            if name in USER_EXCLUDE_ATTRS:
                                continue
                            typ_ = meta.annotation.__name__
                            if meta.is_required():
                                val = pretty_input(
                                    f'{name} (type: {typ_} required): '
                                )
                                values[name] = meta.annotation(val)
                            else:
                                val = pretty_input(
                                    f'{name} (type: {typ_} press '
                                    f'enter to leave empty): '
                                )
                                if val:
                                    values[name] = meta.annotation(val)
                        try:
                            print(f'attr values: {values}')
                            values['id'] = user_id
                            UserModel.model_validate(values, strict=True)
                            del values['id']
                            await repo.create_user(user_id, **values)
                            pretty_print('User successfully created!')
                            try:
                                with input_block('Configure credentials:'):
                                    for i, prov in enumerate(settings.AUTH_PROVIDERS):
                                        name = prov.split('.')[-1]
                                        pretty_input(f'[{i}] - {name}')

                                    while True:
                                        prov_index = pretty_input('select provider: ')
                                        if prov_index.isdigit():
                                            prov_index = int(prov_index)
                                            if prov_index >= len(settings.AUTH_PROVIDERS):
                                                pretty_print('Invalid provider index')
                                            else:
                                                prov = settings.AUTH_PROVIDERS[prov_index]
                                                prov_name = prov.split('.')[-1]
                                                with input_block(f'Configure credentials with provider "{prov_name}"'):
                                                    prov_cls = load_class(settings.AUTH_PROVIDERS[prov_index])
                                                    auth_prov: BaseAuthProvider = prov_cls()
                                                    await self.set_credentials(user_id, auth_prov)
                                                    return
                                        else:
                                            pretty_print('Invalid provider index')
                            except Exception as e:
                                pretty_print(str(e))
                                pretty_print('removing user cause of errors...')
                                await repo.delete_user(user_id)
                            return
                        except ValidationError as e:
                            with input_block('!!!!!! Errors !!!!!!'):
                                for err in e.errors():
                                    pretty_print(f'attr "{err["loc"][0]}" - {err["msg"]}')
                            return

    async def edit_user(self, repo: Repository):
        while True:
            with input_block('Edit user'):
                user_id = pretty_input('Press user id: ')
                user = await repo.get_user(user_id=user_id)
                if user is None:
                    pretty_print(f'No user with id: {user_id}')
                    choice = pretty_input(f'Enter [{END}] to break or press any key to repeat: ')
                    if choice == END:
                        return
                else:
                    pretty_print_user(user)
                    with input_block('Edit sub-commands:'):
                        for cmd, comment in list(EditSubCmd):
                            pretty_print(f'[{cmd}] - {comment}')
                        pretty_print(f'[{END}] - break')
                        choice = pretty_input(f'Your choice: ')
                        if choice == END:
                            return
                        elif choice == EditSubCmd.INFO[0]:
                            await self.edit_user_info(user, repo)


    @staticmethod
    async def edit_user_info(user: UserModel, repo: Repository):
        attrs = UserModel.model_fields
        data_dict = user.model_dump(mode='python')
        values = {'id': user.id}
        with input_block('Edit user info: '):
            for name, meta in attrs.items():
                if name in USER_EXCLUDE_ATTRS:
                    continue
                typ_ = meta.annotation.__name__
                set_val = data_dict.get(name, '')
                value = pretty_input(
                    f'{name} (type: {typ_} press enter to leave value "{set_val}"): '
                )
                if value == '':
                    values[name] = set_val
                else:
                    try:
                        if typ_ == 'bool':
                            values[name] = True if value.lower() == 'true' else False
                        elif typ_ == 'int':
                            values[name] = int(value)
                        elif typ_ == 'float':
                            values[name] = float(value)
                        else:
                            values[name] = value
                    except:
                        values[name] = value
            try:
                print(str(values))
                UserModel.model_validate(values, strict=True)
                new_user = UserModel(**values)
                user = await repo.modify_user(new_user)
                pretty_print_user(user)
                return
            except ValidationError as e:
                with input_block('!!!!!! Errors !!!!!!'):
                    for err in e.errors():
                        pretty_print(f'attr "{err["loc"][0]}" - {err["msg"]}')

    @staticmethod
    async def set_credentials(user_id: str, auth: BaseAuthProvider):
        cred_model = auth.CredModel
        attrs = cred_model.model_fields
        while True:
            values = {}
            for name, meta in attrs.items():
                typ_ = meta.annotation.__name__
                if meta.is_required():
                    values[name] = pretty_input(
                        f'{name} (type: {typ_} required): '
                    )
                else:
                    val = pretty_input(
                        f'{name} (type: {typ_} press '
                        f'enter to leave empty): '
                    )
                    if val:
                        values[name] = val
            #
            cred_model.model_validate(values, strict=True)
            cred = await auth.register_credentials(
                user_id=user_id, rewrite_exists=True, **values
            )
            pretty_print('Credentials successfully created!')
            pretty_print_json(cred.model_dump(mode='json'))
            return
