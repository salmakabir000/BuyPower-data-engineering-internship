import pandas as pd


def read(config):
    return pd.read_parquet(config["path"])
