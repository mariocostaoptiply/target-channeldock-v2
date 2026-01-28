"""Base sink implementation for ChannelDock."""

from __future__ import annotations

import json

import backoff
import requests
from singer_sdk.exceptions import RetriableAPIError
from target_hotglue.sinks import HotglueSink


class ChannelDockBaseSink(HotglueSink):
    """ChannelDock base sink with authentication."""

    @property
    def base_url(self) -> str:
        """Return the base URL for the API."""
        return self.config.get("url_base", "https://channeldock.com/portal/api/v2/")

    @property
    def http_headers(self) -> dict:
        """Return HTTP headers with API key and secret authentication."""
        headers = {
            "Content-Type": "application/json",
            "api_key": self.config.get("api_key"),
            "api_secret": self.config.get("api_secret"),
        }
        return headers

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def request_api(
        self,
        http_method: str,
        endpoint: str,
        params=None,
        request_data=None,
        headers=None,
    ) -> requests.Response:
        """Make an API request with retry logic."""
        # Ensure base URL ends with / and endpoint doesn't start with /
        base_url = self.base_url.rstrip('/') + '/'
        url = f"{base_url}{endpoint.lstrip('/')}"
        
        self.logger.info(f"Making {http_method} request to: {url}")
        
        # Log request payload for debugging
        try:
            self.logger.info(f"Request payload: {json.dumps(request_data, indent=2)}")
        except (TypeError, ValueError):
            self.logger.info(f"Request payload: {request_data}")
        
        request_headers = self.http_headers.copy()
        if headers:
            request_headers.update(headers)

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=request_headers,
            json=request_data,
        )

        # Log response for debugging
        try:
            self.logger.info(f"Response ({response.status_code}): {response.text[:500]}")
        except Exception:
            self.logger.info(f"Response ({response.status_code}): {response.text}")

        if response.status_code in [429, 500, 502, 503, 504]:
            self.logger.error(
                f"API error {response.status_code}: {response.text[:500]}"
            )
            raise RetriableAPIError(f"Retriable error: {response.status_code}")

        response.raise_for_status()
        return response
