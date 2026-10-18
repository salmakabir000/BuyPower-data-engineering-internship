import pandas as pd


class Transform:
    def __init__(self, config):
        self.column = config["column"]
        self.to = config["to"]

    def apply(self, df):
        if self.column not in df.columns:
            raise ValueError(f"cast_column: column '{self.column}' not found in data")
        df = df.copy()
        if self.to == "int":
            df[self.column] = pd.to_numeric(df[self.column], errors="coerce").astype("Int64")
        elif self.to == "float":
            df[self.column] = pd.to_numeric(df[self.column], errors="coerce")
        elif self.to == "str":
            df[self.column] = df[self.column].astype(str)
        elif self.to == "bool":
            df[self.column] = df[self.column].astype(bool)
        elif self.to == "datetime":
            df[self.column] = pd.to_datetime(df[self.column], errors="coerce")
        else:
            raise ValueError(f"cast_column: unsupported target type '{self.to}'")
        return df
