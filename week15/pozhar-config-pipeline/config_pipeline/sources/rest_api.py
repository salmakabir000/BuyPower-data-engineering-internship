"""
rest_api source - fetches data from any REST API that returns JSON.

Handles two real-world JSON response shapes generically (no API-specific
code anywhere):

  1. "records" (the default): the response (or something inside it, via
     response_path) is already a list of row-dicts.
     e.g. CoinGecko's /coins/markets returns [{"id": "bitcoin", ...}, ...]

  2. "columnar": the response (or something inside it) is a dict where
     each key maps to a list of values, all the same length, meant to be
     zipped together into rows.
     e.g. Open-Meteo's hourly data looks like:
     {"hourly": {"time": [...], "temperature_2m": [...]}}

Optional pagination (type: "page") repeatedly requests more pages by
incrementing a query param, stopping at max_pages or on an empty page,
whichever comes first.
"""

import pandas as pd
import requests


class Source:
    def __init__(self, config):
        self.base_url = config["base_url"].rstrip("/")
        self.endpoint = config["endpoint"]
        self.params = dict(config.get("params", {}))
        self.headers = config.get("headers", {})
        self.pagination = config.get("pagination")
        self.response_path = config.get("response_path", "")
        self.response_format = config.get("response_format", "records")
        self.timeout = config.get("timeout_seconds", 15)

    def fetch(self):
        if self.pagination:
            return self._fetch_paginated()
        data = self._request(self.params)
        return self._to_dataframe(data)

    def _fetch_paginated(self):
        param_name = self.pagination["param_name"]
        max_pages = self.pagination.get("max_pages", 1)
        start_page = self.pagination.get("start_page", 1)

        all_rows = []
        for page in range(start_page, start_page + max_pages):
            page_params = dict(self.params)
            page_params[param_name] = page
            data = self._request(page_params)
            page_df = self._to_dataframe(data)
            if page_df.empty:
                break
            all_rows.append(page_df)

        if not all_rows:
            return pd.DataFrame()
        return pd.concat(all_rows, ignore_index=True)

    def _request(self, params):
        url = f"{self.base_url}/{self.endpoint.lstrip('/')}"
        resp = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _navigate(self, data):
        """Walks response_path (e.g. 'hourly' or 'results.data') into the
        parsed JSON to find the part we actually want."""
        if not self.response_path:
            return data
        node = data
        for key in self.response_path.split("."):
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                raise ValueError(
                    f"response_path '{self.response_path}' not found in API response "
                    f"(failed at '{key}')"
                )
        return node

    def _to_dataframe(self, data):
        node = self._navigate(data)

        if self.response_format == "records":
            if not isinstance(node, list):
                raise ValueError(
                    "response_format is 'records' but the data at response_path "
                    f"is a {type(node).__name__}, not a list. "
                    "Set response_path to point at the list, or use response_format: columnar."
                )
            return pd.DataFrame(node)

        if self.response_format == "columnar":
            if not isinstance(node, dict):
                raise ValueError(
                    "response_format is 'columnar' but the data at response_path "
                    f"is a {type(node).__name__}, not a dict of column -> list."
                )
            return pd.DataFrame(node)

        raise ValueError(f"Unknown response_format '{self.response_format}'")
