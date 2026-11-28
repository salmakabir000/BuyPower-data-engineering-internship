import os
from pathlib import Path

import yaml


def load_yaml(file_path):
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r") as file:
        data = yaml.safe_load(file)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML configuration: {path}")

    return data



def load_config(project_path, profiles_path, require_database=True):
    project = load_yaml(project_path)
    profiles = load_yaml(profiles_path)

    required_project_fields = ["name", "profile", "models"]

    for field in required_project_fields:
        if field not in project:
            raise ValueError(f"Missing project configuration: {field}")

    profile_name = project["profile"]

    if profile_name not in profiles:
        raise ValueError(f"Profile '{profile_name}' not found")

    profile = profiles[profile_name]
    target = profile.get("target")

    outputs = profile.get("outputs", {})

    if target not in outputs:
        raise ValueError(
            f"Target '{target}' not found in profile '{profile_name}'"
        )

    database_config = outputs[target]

    if require_database:
        password_env = database_config.get("password_env")

        if not password_env:
            raise ValueError("Missing password_env in database profile")

        password = os.getenv(password_env)

        if not password:
            raise ValueError(
                f"Required environment variable '{password_env}' is not set"
            )

        database_config["password"] = password

    return {
        "project": project,
        "database": database_config,
    }
