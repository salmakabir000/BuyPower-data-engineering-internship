"""Transforms: each one has `apply(df) -> df`."""
from __future__ import annotations

import pandas as pd

from .base import Component, PipelineError, field


class SelectColumns(Component):
    schema = {"columns": field(list, required=True)}

    @classmethod
    def check(cls, config, path):
        cols = config.get("columns")
        if isinstance(cols, list) and (not cols or not all(isinstance(c, str) for c in cols)):
            return [f"{path}.columns: must be a non-empty list of column names"]
        return []

    def apply(self, df):
        missing = [c for c in self.config["columns"] if c not in df.columns]
        if missing:
            raise PipelineError(f"select_columns: columns not found: {missing}. Have: {list(df.columns)}")
        return df[self.config["columns"]].copy()


class RenameColumn(Component):
    schema = {"mapping": field(dict, required=True)}

    def apply(self, df):
        missing = [c for c in self.config["mapping"] if c not in df.columns]
        if missing:
            raise PipelineError(f"rename_column: columns not found: {missing}")
        return df.rename(columns=self.config["mapping"])


class AddColumn(Component):
    schema = {"name": field(str, required=True), "value": field(object, required=True)}

    def apply(self, df):
        df = df.copy()
        df[self.config["name"]] = self.config["value"]
        return df


class FilterRows(Component):
    OPS = ("eq", "ne", "gt", "gte", "lt", "lte", "in", "not_null")
    schema = {
        "column": field(str, required=True),
        "op": field(str, required=True, choices=OPS),
        "value": field(object),
    }

    @classmethod
    def check(cls, config, path):
        op = config.get("op")
        if op and op != "not_null" and config.get("value") is None:
            return [f"{path}.value: required when op is '{op}'"]
        if op == "in" and not isinstance(config.get("value"), list):
            return [f"{path}.value: must be a list when op is 'in'"]
        return []

    def apply(self, df):
        col, op, val = self.config["column"], self.config["op"], self.config["value"]
        if col not in df.columns:
            raise PipelineError(f"filter_rows: column '{col}' not found")
        s = df[col]
        mask = {
            "eq": lambda: s == val, "ne": lambda: s != val,
            "gt": lambda: s > val, "gte": lambda: s >= val,
            "lt": lambda: s < val, "lte": lambda: s <= val,
            "in": lambda: s.isin(val), "not_null": lambda: s.notna(),
        }[op]()
        return df[mask].reset_index(drop=True)


class CastColumn(Component):
    schema = {
        "column": field(str, required=True),
        "dtype": field(str, required=True, choices=("int", "float", "str", "bool", "datetime")),
    }

    def apply(self, df):
        col, dtype = self.config["column"], self.config["dtype"]
        if col not in df.columns:
            raise PipelineError(f"cast_column: column '{col}' not found")
        df = df.copy()
        try:
            if dtype == "int":
                df[col] = pd.to_numeric(df[col]).astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col]).astype("float64")
            elif dtype == "str":
                df[col] = df[col].astype("string")
            elif dtype == "bool":
                df[col] = df[col].astype(bool)
            else:
                df[col] = pd.to_datetime(df[col])
        except (ValueError, TypeError) as exc:
            raise PipelineError(f"cast_column: cannot cast '{col}' to {dtype}: {exc}")
        return df
