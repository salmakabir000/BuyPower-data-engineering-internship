def apply(df, config):
    result = df.copy()
    result[config["column"]] = config["value"]
    return result
