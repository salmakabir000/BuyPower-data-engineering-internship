def apply(df, config):
    return df[config["columns"]].copy()
