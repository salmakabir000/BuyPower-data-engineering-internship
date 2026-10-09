_OPERATORS = {
    "==": lambda s, v: s == v,
    "!=": lambda s, v: s != v,
    ">": lambda s, v: s > v,
    "<": lambda s, v: s < v,
    ">=": lambda s, v: s >= v,
    "<=": lambda s, v: s <= v,
    "in": lambda s, v: s.isin(v),
    "not_null": lambda s, v: s.notna(),
}


class Transform:
    def __init__(self, config):
        self.column = config["column"]
        self.operator = config["operator"]
        self.value = config.get("value")
        if self.operator not in _OPERATORS:
            raise ValueError(
                f"filter_rows: unknown operator '{self.operator}'. "
                f"Supported: {list(_OPERATORS)}"
            )

    def apply(self, df):
        if self.column not in df.columns:
            raise ValueError(f"filter_rows: column '{self.column}' not found in data")
        mask = _OPERATORS[self.operator](df[self.column], self.value)
        return df[mask]
