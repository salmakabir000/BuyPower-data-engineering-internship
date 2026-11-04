import unittest
from unittest.mock import Mock, patch
from contextlib import redirect_stdout
from io import StringIO

import api2sql


class TestApi2SQL(unittest.TestCase):

    def test_get_latest_cursor(self):
        records = [
            {"updated_at": "2026-11-01T10:00:00Z"},
            {"updated_at": "2026-11-03T10:00:00Z"},
            {"updated_at": "2026-11-02T10:00:00Z"},
        ]

        result = api2sql.get_latest_cursor(records, "updated_at")

        self.assertEqual(result, "2026-11-03T10:00:00Z")

    def test_get_latest_cursor_empty_records(self):
        result = api2sql.get_latest_cursor([], "updated_at")

        self.assertIsNone(result)

    def test_get_next_link(self):
        response = Mock()
        response.headers = {
            "Link": '<https://api.example.com/issues?page=2>; rel="next", '
                    '<https://api.example.com/issues?page=5>; rel="last"'
        }

        result = api2sql.get_next_link(response)

        self.assertEqual(
            result,
            "https://api.example.com/issues?page=2"
        )

    def test_get_next_link_without_next_page(self):
        response = Mock()
        response.headers = {}

        result = api2sql.get_next_link(response)

        self.assertIsNone(result)

    @patch.dict("os.environ", {}, clear=True)
    def test_get_env_missing_variable(self):
        with self.assertRaises(RuntimeError):
            api2sql.get_env("MISSING_VARIABLE")

    @patch("api2sql.load_config")
    def test_validate_config(self, mock_load_config):
        mock_load_config.return_value = {
            "name": "github_issues",
            "source": {
                "type": "rest_api",
                "base_url": "https://api.github.com",
                "endpoint": "/repos/apache/airflow/issues",
                "pagination": {
                    "type": "link_header"
                },
            },
            "destination": {
                "type": "postgres",
                "conn_env": "PG_CONN",
                "table": "raw_github_issues",
                "primary_key": "id",
                "write_mode": "upsert",
            },
        }

        output = StringIO()

        with redirect_stdout(output):
            api2sql.validate_config("test.yml")

        self.assertIn("Configuration is valid.", output.getvalue())


if __name__ == "__main__":
    unittest.main()