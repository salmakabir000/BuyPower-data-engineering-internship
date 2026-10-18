def apply(df, config):
    return df.rename(columns=config["mapping"])
