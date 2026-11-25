import pytest

from mini_dbt.config import load_config


def test_load_config(tmp_path, monkeypatch):
    project_file = tmp_path / "project.yml"
    profiles_file = tmp_path / "profiles.yml"

    project_file.write_text("""
name: github_analytics
profile: github_warehouse
models:
  schema: analytics
  materialized: table
""")

    profiles_file.write_text("""
github_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: warehouse_loader
      database: github_warehouse
      schema: analytics
      password_env: DB_PASSWORD
""")

    monkeypatch.setenv("DB_PASSWORD", "test_password")

    config = load_config(project_file, profiles_file)

    assert config["project"]["name"] == "github_analytics"
    assert config["database"]["database"] == "github_warehouse"
    assert config["database"]["password"] == "test_password"


def test_missing_password_fails(tmp_path, monkeypatch):
    project_file = tmp_path / "project.yml"
    profiles_file = tmp_path / "profiles.yml"

    project_file.write_text("""
name: github_analytics
profile: github_warehouse
models:
  schema: analytics
""")

    profiles_file.write_text("""
github_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      password_env: DB_PASSWORD
""")

    monkeypatch.delenv("DB_PASSWORD", raising=False)

    with pytest.raises(ValueError, match="DB_PASSWORD"):
        load_config(project_file, profiles_file)
