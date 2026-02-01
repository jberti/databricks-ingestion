import os
import requests
from requests.adapters import HTTPAdapter
from src.helpers.custom_retry import CustomRetry
from src.config import Config
from typing import Optional
from ratelimit import limits, sleep_and_retry


class PokeAPIService:
    def __init__(
        self,
        timeout: int = None,
        forcelist: list = [408, 409, 413, 423, 429, 500, 502, 503, 504, 520],
        retries_total: int = None,
        backoff_factor: float = 0.25,
        pool_connections: int = 3,
        pool_maxsize: int = 3,
    ) -> None:
        self.config = Config()
        self.requests = 0
        self._timeout = timeout or self.config.POKEAPI_TIMEOUT
        self._retries_total = retries_total or self.config.POKEAPI_MAX_RETRIES
        self._backoff_factor = backoff_factor
        self._rate_limit = self.config.POKEAPI_RATE_LIMIT
        self._base_url = self.config.POKEAPI_BASE_URL
        
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        self._adapter = HTTPAdapter(
            max_retries=CustomRetry(
                total=self._retries_total,
                read=self._retries_total,
                connect=self._retries_total,
                status=self._retries_total,
                other=self._retries_total,
                backoff_factor=backoff_factor,
                status_forcelist=forcelist,
                raise_on_status=False,
                allowed_methods=None,
            ),
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
        )

    @sleep_and_retry
    @limits(calls=1, period=1)  # Will be dynamically set based on rate_limit
    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict = {},
    ) -> Optional[requests.Response]:
        # Apply dynamic rate limiting
        if self._rate_limit > 0:
            import time
            time.sleep(1.0 / self._rate_limit)
        
        session = None
        response = None

        session = requests.Session()
        session.mount(self._base_url, self._adapter)

        try:
            self.requests += 1
            print(f"Making request #{self.requests} to {endpoint} with params {params}")
            response = session.request(
                method=method,
                url=f"{self._base_url}{endpoint}",
                headers=self._headers,
                params=params,
                timeout=self._timeout,
            )

        except Exception as e:
            raise e
        finally:
            if session:
                session.close()

        return response