import pandas as pd
import requests


def read(config):
    response = requests.get(
        config["url"],
        params=config.get("params"),
        timeout=config.get("timeout", 30),
    )
    response.raise_for_status()
    payload = response.json()

    data_path = config.get("data_path")
    if data_path:
        for key in data_path.split("."):
            if not isinstance(payload, dict) or key not in payload:
                raise ValueError(f"API response does not contain data path: {data_path}")
            payload = payload[key]

    if isinstance(payload, list):
        return pd.json_normalize(payload)

    if isinstance(payload, dict):
        if all(isinstance(value, list) for value in payload.values()):
            return pd.DataFrame(payload)
        return pd.json_normalize(payload)

    raise ValueError("API response must contain a JSON list or object.")
