"""HTTP client for the mock API used by the ingestion layer."""

import json
import logging
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.config import API_URL

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class APIClient:
    """Small client responsible only for communicating with the source API."""

    def __init__(
        self,
        base_url: str = API_URL,
        timeout: int = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Perform a GET request and return the decoded JSON response."""

        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"

        for attempt in range(self.retries + 1):
            request = Request(
                url,
                headers={"Accept": "application/json"},
                method="GET",
            )

            try:
                with urlopen(request, timeout=self.timeout) as response:
                    return json.load(response)

            except HTTPError as exc:
                if exc.code not in RETRYABLE_STATUS_CODES:
                    raise

                if attempt >= self.retries:
                    raise

                logger.warning(
                    "API request failed with HTTP %s; retrying (%s/%s)",
                    exc.code,
                    attempt + 1,
                    self.retries,
                )

            except URLError:
                if attempt >= self.retries:
                    raise

                logger.warning(
                    "API request could not reach %s; retrying (%s/%s)",
                    url,
                    attempt + 1,
                    self.retries,
                )

            time.sleep(2**attempt)

        raise RuntimeError("API request failed unexpectedly")

    def get_orders(self, limit: int = 100) -> list[dict[str, Any]]:
        """Fetch orders from the source API."""
        return self._get("/orders", {"limit": limit})

    def get_deliveries(self, limit: int = 100) -> list[dict[str, Any]]:
        """Fetch deliveries from the source API."""
        return self._get("/delivery", {"limit": limit})

    def get_inventory(
        self,
        snapshot_date: str | None = None,
        product_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch an inventory snapshot, optionally filtered by date/product."""
        params: dict[str, Any] = {}

        if snapshot_date is not None:
            params["snapshot_date"] = snapshot_date

        if product_id is not None:
            params["product_id"] = product_id

        return self._get("/inventory", params)
