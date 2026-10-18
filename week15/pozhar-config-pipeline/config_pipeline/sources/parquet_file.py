import pandas as pd


class Source:
    def __init__(self, config):
        self.path = config["path"]

    def fetch(self):
        return pd.read_parquet(self.path)
