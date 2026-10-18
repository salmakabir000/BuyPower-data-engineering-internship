def apply(df, config):
    column = config["column"]
    operator = config["operator"]
    value = config.get("value")

    if operator == "equals":
        mask = df[column] == value
    elif operator == "not_equals":
        mask = df[column] != value
    elif operator == "greater_than":
        mask = df[column] > value
    elif operator == "less_than":
        mask = df[column] < value
    elif operator == "is_not_null":
        mask = df[column].notna()
    else:
        raise ValueError(f"Unsupported filter operator: {operator}")

    return df[mask].copy()
