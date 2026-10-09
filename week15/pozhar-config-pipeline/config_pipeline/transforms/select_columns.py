class Transform:
    def __init__(self, config):
        self.columns = config["columns"]

    def apply(self, df):
        missing = [c for c in self.columns if c not in df.columns]
        if missing:
            raise ValueError(f"select_columns: columns not found in data: {missing}")
        return df[self.columns]
