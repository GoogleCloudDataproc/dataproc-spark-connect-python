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

import re
from typing import Optional

from IPython.core import magic_arguments
from IPython.core.magic import Magics, cell_magic, line_cell_magic, magics_class
from IPython.display import HTML, display
from traitlets import Int

from pyspark.sql import DataFrame, SparkSession
from google.cloud.dataproc_spark_connect import DataprocSparkSession


def get_active_spark_session() -> Optional[SparkSession]:
    """Get the active Spark session, preferring DataprocSparkSession."""
    try:
        from pyspark.sql import SparkSession

        session = SparkSession.getActiveSession()
        if session:
            return session

        try:
            from IPython import get_ipython

            ipython = get_ipython()
            if ipython:
                spark = ipython.user_ns.get("spark")
                if spark is not None:
                    # Check if it's a Spark session object by checking for sql method
                    if hasattr(spark, "sql") and callable(
                        getattr(spark, "sql")
                    ):
                        return spark
        except (NameError, AttributeError, ImportError):
            pass

    except ImportError:
        pass

    return None


def interpolate_variables(query: str, user_namespace: dict) -> str:
    """Replace {variable} placeholders in the query with actual values."""

    def replacer(match):
        var_name = match.group(1)
        if var_name in user_namespace:
            return str(user_namespace[var_name])
        return match.group(0)

    pattern = r"\{(\w+)\}"
    return re.sub(pattern, replacer, query)


def format_dataframe_as_html(df: DataFrame, limit: int = 20) -> str:
    """Format a DataFrame as an HTML table for notebook display."""
    pandas_df = df.limit(limit).toPandas()

    html = "<table border='1'>\n"
    html += (
        "<tr>"
        + "".join(f"<th>{col}</th>" for col in pandas_df.columns)
        + "</tr>\n"
    )

    for _, row in pandas_df.iterrows():
        html += "<tr>" + "".join(f"<td>{val}</td>" for val in row) + "</tr>\n"

    html += "</table>"

    if df.count() > limit:
        html += f"<p><i>only showing top {limit} row(s)</i></p>"

    return html


@magics_class
class DataprocSparkSql(Magics):
    """Magic commands for executing Spark SQL queries in notebooks."""

    limit = Int(
        20,
        config=True,
        help="The maximum number of rows to display (default: 20)",
    )

    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument(
        "variable",
        nargs="?",
        help="Capture the result in a variable",
    )
    @magic_arguments.argument(
        "-c",
        "--cache",
        action="store_true",
        help="Cache the resulting DataFrame",
    )
    @magic_arguments.argument(
        "-e",
        "--eager",
        action="store_true",
        help="Cache the DataFrame with eager loading",
    )
    @magic_arguments.argument(
        "-v",
        "--view",
        type=str,
        help="Create a temporary view with the specified name",
    )
    @magic_arguments.argument(
        "-l",
        "--limit",
        type=int,
        help="Override the default row display limit",
    )
    def sparksql(self, line: str, cell: str) -> Optional[DataFrame]:
        """
        Execute a Spark SQL query and display the results.

        Usage:
            %%sparksql [options] [variable]
            SELECT * FROM table

        Options:
            -c, --cache : Cache the resulting DataFrame
            -e, --eager : Cache with eager loading
            -v VIEW, --view VIEW : Create a temporary view
            -l LIMIT, --limit LIMIT : Override default row limit
            variable : Store result in a variable

        Examples:
            %%sparksql
            SELECT * FROM users

            %%sparksql df
            SELECT * FROM users WHERE age > 21

            %%sparksql --cache --view adults df
            SELECT * FROM users WHERE age >= 18
        """
        args = magic_arguments.parse_argstring(self.sparksql, line)

        spark = get_active_spark_session()
        if not spark:
            raise RuntimeError(
                "No active Spark session found. Please create a DataprocSparkSession or SparkSession first."
            )

        query = interpolate_variables(cell.strip(), self.shell.user_ns)

        if not query:
            raise ValueError("SQL query cannot be empty")

        try:
            df = spark.sql(query)

            if args.cache or args.eager:
                if args.eager:
                    df.cache().count()
                    print("cache dataframe with eager load")
                else:
                    df.cache()
                    print("cache dataframe")

            if args.view:
                df.createOrReplaceTempView(args.view)
                print(f"create temporary view '{args.view}'")

            if args.variable:
                self.shell.user_ns[args.variable] = df
                print(f"return dataframe to local variable 'df'")

            display_limit = args.limit if args.limit is not None else self.limit

            if display_limit > 0:
                html_output = format_dataframe_as_html(df, display_limit)
                display(HTML(html_output))

            return df if args.variable else None

        except Exception as e:
            raise RuntimeError(f"Error executing SQL query: {str(e)}") from e

    @line_cell_magic
    def config(self, line: str, cell: str = "") -> None:  # noqa: ARG002
        """
        Configure SparkSQL magic settings.

        Usage:
            %config SparkSql.limit=50

        Example:
            %config SparkSql.limit=100
        """
        if line:
            parts = line.strip().split("=")
            if len(parts) == 2 and parts[0].startswith("SparkSql."):
                key = parts[0].replace("SparkSql.", "")
                value = parts[1]

                if key == "limit":
                    try:
                        self.limit = int(value)
                        print(f"SparkSql.limit set to {self.limit}")
                    except ValueError:
                        print(
                            f"Invalid value for limit: {value}. Must be an integer."
                        )
                else:
                    print(f"Unknown configuration key: {key}")
            else:
                print("Usage: %config SparkSql.<key>=<value>")
        else:
            print(f"Current settings:")
            print(f"  SparkSql.limit = {self.limit}")
