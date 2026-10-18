def apply(df, config):
    result = df.copy()
    column = config["column"]
    dtype = config["dtype"]

    allowed_types = {"string", "integer", "float", "boolean"}

    if dtype not in allowed_types:
        raise ValueError(f"Unsupported data type: {dtype}")

    type_map = {
        "string": "string",
        "integer": "Int64",
        "float": "float64",
        "boolean": "boolean",
    }

    result[column] = result[column].astype(type_map[dtype])
    return result
