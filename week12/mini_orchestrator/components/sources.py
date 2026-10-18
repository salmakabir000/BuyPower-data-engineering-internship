import urllib.parse
import urllib.request
import json
import pandas as pd


class rest_api:
    def __init__(self, config):
        self.config = config

    def extract(self):
        base_url = self.config["base_url"]
        endpoint = self.config["endpoint"]
        params = self.config.get("params", {})
        pagination = self.config.get("pagination")

        all_data = []

        if pagination:
            max_pages = pagination.get("max_pages", 1)
            param_name = pagination.get("param_name", "page")

            pages = range(1, max_pages + 1)
        else:
            pages = [None]

        for page in pages:
            page_params = dict(params)

            if page is not None:
                page_params[param_name] = page

            url = (
                base_url
                + endpoint
                + "?"
                + urllib.parse.urlencode(page_params)
            )

            print(f"Fetching: {url}")

            with urllib.request.urlopen(url) as response:
                data = json.loads(
                    response.read().decode("utf-8")
                )

            if isinstance(data, list):
                all_data.extend(data)

            elif isinstance(data, dict):
                all_data.append(data)

            else:
                raise ValueError(
                    "API response must be a JSON list or object"
                )

        return pd.json_normalize(all_data)


class csv_file:
    def __init__(self, config):
        self.config = config

    def extract(self):
        return pd.read_csv(self.config["path"])


class parquet_file:
    def __init__(self, config):
        self.config = config

    def extract(self):
        return pd.read_parquet(self.config["path"])