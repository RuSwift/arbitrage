import os
import contextlib
import datetime
import base64
import pickle
from typing import Optional, ClassVar, Any


def utc_now_float() -> float:
    now = datetime.datetime.utcnow()
    return datetime_to_float(now)


def datetime_to_float(d) -> float:
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds = (d - epoch).total_seconds()
    return total_seconds


def float_to_datetime(fl) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(fl)


def datetime_delta(
        t1: Optional[float], t2: Optional[float]
) -> Optional[datetime.timedelta]:
    if not (t1 and t2):
        return None
    if t1 > t2:
        diff = float_to_datetime(t1) - float_to_datetime(t2)
    else:
        diff = float_to_datetime(t2) - float_to_datetime(t1)
    return diff


def load_class(class_path: str) -> ClassVar:
    components = class_path.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


@contextlib.contextmanager
def print_timeout(msg: str = None):
    if not msg:
        msg = 'Timeout: '
    start = datetime.datetime.now()
    yield
    delta = datetime.datetime.now() - start
    timeout = delta.total_seconds()
    print(msg + str(timeout))


def serialize_to_base64(o: Any) -> str:
    b = pickle.dumps(o)
    return base64.standard_b64encode(b).decode()


def deserialize_from_base65(b64: str) -> Any:
    b = base64.standard_b64decode(b64.encode())
    return pickle.loads(b)


def terminate_application(exit_code: int):
    os._exit(exit_code)
