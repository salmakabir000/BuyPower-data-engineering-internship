class Transform:
    def __init__(self, config):
        # config keys are "from" and "to" - "from" is a reserved Python
        # keyword, which is exactly why every component here takes a plain
        # dict instead of **kwargs. We just read it as a dict key instead.
        self.from_col = config["from"]
        self.to_col = config["to"]

    def apply(self, df):
        if self.from_col not in df.columns:
            raise ValueError(f"rename_column: column '{self.from_col}' not found in data")
        return df.rename(columns={self.from_col: self.to_col})
