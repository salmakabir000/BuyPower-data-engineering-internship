import pandas as pd


def read(config):
    path = config["path"]
    return pd.read_csv(path)
