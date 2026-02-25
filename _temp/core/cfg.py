import json
from typing import Type, Optional

import pydantic
from pydantic_yaml import parse_yaml_raw_as, to_yaml_str


class CfgLoader:

    class BaseCfgModel(pydantic.BaseModel, extra=pydantic.Extra.allow):
        ...

    Model = BaseCfgModel

    def __init__(self, cfg: 'Model' = None):
        self._cfg = cfg

    @property
    def cfg(self) -> Optional['Model']:
        return self._cfg

    def to_json(self) -> str:
        if self.cfg:
            return json.dumps(self.cfg.model_dump(mode='json'), indent=2)
        else:
            return ''

    def from_json(self, s: str, section: str = None):
        if section:
            parent_cfg = json.loads(s)
            values = parent_cfg.get(section, {})
            self._cfg = self.Model.model_validate(values)
        else:
            obj = json.loads(s)
            self._cfg = self.Model.model_validate(obj)

    def to_yaml(self) -> str:
        if self.cfg:
            return to_yaml_str(self.cfg, indent=2)
        else:
            return ''

    def from_yaml(self, s: str, section: str = None):
        if section:
            parent_cfg = parse_yaml_raw_as(self.BaseCfgModel, s)
            values = getattr(parent_cfg, section)
            self._cfg = self.Model.model_validate(values)
        else:
            self._cfg = parse_yaml_raw_as(self.Model, s)
