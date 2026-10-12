"""Sources: each one has a `read()` that returns a pandas DataFrame."""
from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

from .base import Component, PipelineError, field


class RestApiSource(Component):
    schema = {
        "base_url": field(str, required=True),
        "endpoint": field(str, default=""),
        "params": field(dict, default={}),
        "headers": field(dict, default={}),
        "records_path": field(str),  # dotted path to the data inside the JSON, e.g. "hourly"
        "pagination": field(dict),
        "timeout": field(int, float, default=30),
        "retries": field(int, default=3),
    }
    _PAGINATION = {
        "type": field(str, required=True, choices=("page",)),
        "param_name": field(str, required=True),
        "max_pages": field(int, required=True),
        "start_page": field(int, default=1),
    }

    @classmethod
    def check(cls, config, path):
        from .base import validate_fields

        pag = config.get("pagination")
        if pag is None:
            return []
        errors = validate_fields(cls._PAGINATION, pag, f"{path}.pagination")
        if isinstance(pag.get("max_pages"), int) and pag["max_pages"] < 1:
            errors.append(f"{path}.pagination.max_pages: must be at least 1")
        return errors

    def read(self) -> pd.DataFrame:
        cfg = self.config
        url = cfg["base_url"].rstrip("/") + "/" + cfg["endpoint"].lstrip("/")
        url = url.rstrip("/")
        pag = cfg["pagination"]

        if not pag:
            return self._to_frame(self._get(url, dict(cfg["params"])))

        frames = []
        start = pag.get("start_page", 1)
        for page in range(start, start + pag["max_pages"]):
            params = {**cfg["params"], pag["param_name"]: page}
            frame = self._to_frame(self._get(url, params))
            if frame.empty:  # ran out of data before max_pages
                break
            frames.append(frame)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _get(self, url: str, params: dict) -> Any:
        last = None
        for attempt in range(self.config["retries"]):
            try:
                resp = requests.get(
                    url, params=params, headers=self.config["headers"],
                    timeout=self.config["timeout"],
                )
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last = exc
                time.sleep(min(2 ** attempt, 8))
        raise PipelineError(f"request to {url} failed after {self.config['retries']} tries: {last}")

    def _to_frame(self, payload: Any) -> pd.DataFrame:
        node = payload
        if self.config["records_path"]:
            for part in self.config["records_path"].split("."):
                if not isinstance(node, dict) or part not in node:
                    raise PipelineError(f"records_path '{self.config['records_path']}': '{part}' not found in response")
                node = node[part]
        if isinstance(node, list):
            return pd.json_normalize(node) if node else pd.DataFrame()
        if isinstance(node, dict):
            if node and all(isinstance(v, list) for v in node.values()):
                return pd.DataFrame(node)  # columnar JSON: {"col": [..], "col2": [..]}
            return pd.json_normalize([node])  # a single record
        raise PipelineError(f"cannot turn a {type(node).__name__} response into rows; set records_path")


class CsvFileSource(Component):
    schema = {
        "path": field(str, required=True),
        "delimiter": field(str, default=","),
        "encoding": field(str, default="utf-8"),
    }

    def read(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.config["path"], sep=self.config["delimiter"],
                               encoding=self.config["encoding"])
        except FileNotFoundError:
            raise PipelineError(f"CSV file not found: {self.config['path']}")


class ParquetFileSource(Component):
    schema = {
        "path": field(str, required=True),
        "columns": field(list),
    }

    def read(self) -> pd.DataFrame:
        try:
            return pd.read_parquet(self.config["path"], columns=self.config["columns"])
        except FileNotFoundError:
            raise PipelineError(f"Parquet file not found: {self.config['path']}")
