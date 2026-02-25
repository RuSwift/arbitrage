from typing import List, Union

from pydantic import BaseModel


class CRUD:

    Model = Union[BaseModel, List[BaseModel]]

    async def _on_created(self, target: Model):
        pass

    async def _on_deleted(self, target: Model):
        pass

    async def _on_modified(self, target: Model):
        pass
