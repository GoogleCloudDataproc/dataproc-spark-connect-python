# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest.mock import MagicMock, Mock, patch

from google.cloud.dataproc_spark_connect.magic.sparksql import (
    DataprocSparkSql,
    interpolate_variables,
    format_dataframe_as_html,
)


class TestSparkSqlMagicUtilities(unittest.TestCase):

    def test_interpolate_variables(self):
        query = (
            "SELECT * FROM table WHERE id = {user_id} AND name = '{user_name}'"
        )
        namespace = {"user_id": 123, "user_name": "Alice"}

        result = interpolate_variables(query, namespace)
        expected = "SELECT * FROM table WHERE id = 123 AND name = 'Alice'"
        self.assertEqual(result, expected)

    def test_interpolate_variables_missing_var(self):
        query = "SELECT * FROM table WHERE id = {missing_var}"
        namespace = {"other_var": 123}

        result = interpolate_variables(query, namespace)
        self.assertEqual(result, "SELECT * FROM table WHERE id = {missing_var}")

    def test_format_dataframe_as_html(self):
        mock_df = MagicMock()
        mock_pandas_df = MagicMock()

        mock_pandas_df.columns = ["id", "name"]
        mock_pandas_df.iterrows.return_value = [
            (0, [1, "Alice"]),
            (1, [2, "Bob"]),
        ]

        mock_limited_df = MagicMock()
        mock_limited_df.toPandas.return_value = mock_pandas_df
        mock_df.limit.return_value = mock_limited_df
        mock_df.count.return_value = 2

        html = format_dataframe_as_html(mock_df, limit=20)

        self.assertIn("<table", html)
        self.assertIn("<th>id</th>", html)
        self.assertIn("<th>name</th>", html)
        self.assertIn("<td>Alice</td>", html)
        self.assertIn("<td>Bob</td>", html)
        self.assertNotIn("only showing top", html)

    def test_format_dataframe_as_html_with_limit(self):
        mock_df = MagicMock()
        mock_pandas_df = MagicMock()

        mock_pandas_df.columns = ["id"]
        mock_pandas_df.iterrows.return_value = [(0, [1])]

        mock_limited_df = MagicMock()
        mock_limited_df.toPandas.return_value = mock_pandas_df
        mock_df.limit.return_value = mock_limited_df
        mock_df.count.return_value = 100

        html = format_dataframe_as_html(mock_df, limit=1)

        self.assertIn("only showing top 1 row(s)", html)


class TestDataprocSparkSqlMagic(unittest.TestCase):

    def test_magic_class_exists(self):
        # Just test that the class can be imported
        self.assertTrue(DataprocSparkSql is not None)

    def test_magic_has_sparksql_method(self):
        # Test that the sparksql method exists
        self.assertTrue(hasattr(DataprocSparkSql, "sparksql"))
        self.assertTrue(callable(getattr(DataprocSparkSql, "sparksql")))

    def test_magic_has_config_method(self):
        # Test that the config method exists
        self.assertTrue(hasattr(DataprocSparkSql, "config"))
        self.assertTrue(callable(getattr(DataprocSparkSql, "config")))


if __name__ == "__main__":
    unittest.main()
