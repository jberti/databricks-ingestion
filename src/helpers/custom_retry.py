from urllib3.util.retry import Retry
from typing import Any

class CustomRetry(Retry):
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

    def increment(self, method = None, url = None, response = None, error = None, _pool = None, _stacktrace = None):
        """Custom increment method to log retry attempts."""

        if response:
            if (
                False ## Replace with custom condition if needed
            ):
                return Retry(
                    total=0,
                    read=0,
                    connect=0,
                    status=0,
                    other=0,
                    backoff_factor=self.backoff_factor,
                    status_forcelist=self.status_forcelist,
                    allowed_methods=self.allowed_methods,
                    raise_on_status=self.raise_on_status,
                )
        return super().increment(method, url, response, error, _pool, _stacktrace)