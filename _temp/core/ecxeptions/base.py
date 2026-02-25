class BaseAPIError(Exception):
    """Base class for all API errors."""

    code = 0

    def __init__(self, message: str = None, *args, **kwargs):
        if message:
            args = [message] + list(args)
        else:
            args = args
        super().__init__(*args, **kwargs)

    @property
    def message(self) -> str:
        return self.args[0]


class AuthAPIError(BaseAPIError):
    code = 1
