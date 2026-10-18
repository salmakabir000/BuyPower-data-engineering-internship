from datetime import datetime


class select_columns:
    def __init__(self, config):
        self.config = config

    def apply(self, data):
        columns = self.config["columns"]
        return data[columns]


class rename_column:
    def __init__(self, config):
        self.config = config

    def apply(self, data):
        old_name = self.config["old_name"]
        new_name = self.config["new_name"]

        return data.rename(
            columns={old_name: new_name}
        )


class add_column:
    def __init__(self, config):
        self.config = config

    def apply(self, data):
        name = self.config["name"]
        value = self.config["value"]

        if value == "{{ now() }}":
            data[name] = datetime.now().isoformat()
        else:
            data[name] = value

        return data


class filter_rows:
    def __init__(self, config):
        self.config = config

    def apply(self, data):
        column = self.config["column"]
        operator = self.config["operator"]
        value = self.config["value"]

        if operator == "==":
            return data[data[column] == value]

        if operator == "!=":
            return data[data[column] != value]

        if operator == ">":
            return data[data[column] > value]

        if operator == "<":
            return data[data[column] < value]

        if operator == ">=":
            return data[data[column] >= value]

        if operator == "<=":
            return data[data[column] <= value]

        raise ValueError(
            f"Unsupported filter operator: {operator}"
        )


class cast_column:
    def __init__(self, config):
        self.config = config

    def apply(self, data):
        column = self.config["column"]
        data_type = self.config["data_type"]

        data[column] = data[column].astype(data_type)

        return data