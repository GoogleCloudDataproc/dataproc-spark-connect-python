# Copyright 2024 Google LLC
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
import datetime
import os
import pytest
import uuid
import certifi

from google.api_core import client_options
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import (
    CreateSessionTemplateRequest,
    DeleteSessionRequest,
    DeleteSessionTemplateRequest,
    GetSessionRequest,
    GetSessionTemplateRequest,
    Session,
    SessionControllerClient,
    SessionTemplate,
    SessionTemplateControllerClient,
    TerminateSessionRequest,
)
from pyspark.errors.exceptions import connect as connect_exceptions
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


_SERVICE_ACCOUNT_KEY_FILE_ = "service_account_key.json"


@pytest.fixture(params=[None, "3.0"])
def image_version(request):
    return request.param


@pytest.fixture
def test_project():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture
def auth_type(request):
    return getattr(request, "param", "SERVICE_ACCOUNT")


@pytest.fixture
def test_region():
    return os.getenv("GOOGLE_CLOUD_REGION")


@pytest.fixture
def test_subnet():
    return os.getenv("DATAPROC_SPARK_CONNECT_SUBNET")


@pytest.fixture
def test_subnetwork_uri(test_subnet):
    # Make DATAPROC_SPARK_CONNECT_SUBNET the full URI to align with how user would specify it in the project
    return test_subnet


@pytest.fixture
def os_environment(auth_type, image_version, test_project, test_region):
    original_environment = dict(os.environ)
    if os.path.isfile(_SERVICE_ACCOUNT_KEY_FILE_):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            _SERVICE_ACCOUNT_KEY_FILE_
        )
    os.environ["DATAPROC_SPARK_CONNECT_AUTH_TYPE"] = auth_type
    if auth_type == "END_USER_CREDENTIALS":
        os.environ.pop("DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT")
    # Add SSL certificate fix
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
    yield os.environ
    os.environ.clear()
    os.environ.update(original_environment)


@pytest.fixture
def api_endpoint(test_region):
    return os.getenv(
        "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
        f"{test_region}-dataproc.googleapis.com",
    )


@pytest.fixture
def test_client_options(api_endpoint, os_environment):
    return client_options.ClientOptions(api_endpoint=api_endpoint)


@pytest.fixture
def session_controller_client(test_client_options):
    return SessionControllerClient(client_options=test_client_options)


@pytest.fixture
def session_template_controller_client(test_client_options):
    return SessionTemplateControllerClient(client_options=test_client_options)


@pytest.fixture
def connect_session(test_project, test_region, os_environment):
    return DataprocSparkSession.builder.getOrCreate()


@pytest.fixture
def session_name(test_project, test_region, connect_session):
    return f"projects/{test_project}/locations/{test_region}/sessions/{DataprocSparkSession._active_s8s_session_id}"


@pytest.mark.parametrize("auth_type", ["END_USER_CREDENTIALS"], indirect=True)
def test_create_spark_session_with_default_notebook_behavior(
    auth_type, connect_session, session_name, session_controller_client
):
    """Test creating a Spark session with default notebook behavior using end user credentials."""
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)
    assert session.state == Session.State.ACTIVE

    df = connect_session.createDataFrame([(1, "Sarah"), (2, "Maria")]).toDF(
        "id", "name"
    )
    assert str(df) == "DataFrame[id: bigint, name: string]"
    connect_session.sql("DROP TABLE IF EXISTS FOO")
    connect_session.sql("CREATE TABLE FOO (bar long, baz long) USING PARQUET")
    with pytest.raises(connect_exceptions.AnalysisException) as ex:
        connect_session.sql(
            "CREATE TABLE FOO (bar long, baz long) USING PARQUET"
        )

        assert "[TABLE_OR_VIEW_ALREADY_EXISTS]" in str(ex)

    assert DataprocSparkSession._active_s8s_session_uuid is not None
    connect_session.sql("DROP TABLE IF EXISTS FOO")
    connect_session.stop()
    session = session_controller_client.get_session(get_session_request)

    assert session.state in [
        Session.State.TERMINATING,
        Session.State.TERMINATED,
    ]
    assert DataprocSparkSession._active_s8s_session_uuid is None


def test_reuse_s8s_spark_session(
    connect_session, session_name, session_controller_client
):
    """Test that Spark sessions can be reused within the same process."""
    assert DataprocSparkSession._active_s8s_session_uuid is not None

    first_session_id = DataprocSparkSession._active_s8s_session_id
    first_session_uuid = DataprocSparkSession._active_s8s_session_uuid

    connect_session = DataprocSparkSession.builder.getOrCreate()
    second_session_id = DataprocSparkSession._active_s8s_session_id
    second_session_uuid = DataprocSparkSession._active_s8s_session_uuid

    assert first_session_id == second_session_id
    assert first_session_uuid == second_session_uuid
    assert DataprocSparkSession._active_s8s_session_uuid is not None
    assert DataprocSparkSession._active_s8s_session_id is not None

    connect_session.stop()


def test_stop_spark_session_with_deleted_serverless_session(
    connect_session, session_name, session_controller_client
):
    """Test stopping a Spark session when the serverless session has been deleted."""
    assert DataprocSparkSession._active_s8s_session_uuid is not None

    delete_session_request = DeleteSessionRequest()
    delete_session_request.name = session_name
    operation = session_controller_client.delete_session(delete_session_request)
    operation.result()
    connect_session.stop()

    assert DataprocSparkSession._active_s8s_session_uuid is None
    assert DataprocSparkSession._active_s8s_session_id is None


def test_stop_spark_session_with_terminated_serverless_session(
    connect_session, session_name, session_controller_client
):
    """Test stopping a Spark session when the serverless session has been terminated."""
    assert DataprocSparkSession._active_s8s_session_uuid is not None

    terminate_session_request = TerminateSessionRequest()
    terminate_session_request.name = session_name
    operation = session_controller_client.terminate_session(
        terminate_session_request
    )
    operation.result()
    connect_session.stop()

    assert DataprocSparkSession._active_s8s_session_uuid is None
    assert DataprocSparkSession._active_s8s_session_id is None


def test_get_or_create_spark_session_with_terminated_serverless_session(
    test_project,
    test_region,
    connect_session,
    session_name,
    session_controller_client,
):
    """Test creating a new Spark session when the previous serverless session has been terminated."""
    first_session_name = session_name

    assert DataprocSparkSession._active_s8s_session_uuid is not None

    first_session = DataprocSparkSession._active_s8s_session_uuid
    terminate_session_request = TerminateSessionRequest()
    terminate_session_request.name = first_session_name
    operation = session_controller_client.terminate_session(
        terminate_session_request
    )
    operation.result()
    connect_session = DataprocSparkSession.builder.getOrCreate()
    second_session = DataprocSparkSession._active_s8s_session_uuid
    second_session_name = f"projects/{test_project}/locations/{test_region}/sessions/{DataprocSparkSession._active_s8s_session_id}"

    assert first_session != second_session
    assert DataprocSparkSession._active_s8s_session_uuid is not None
    assert DataprocSparkSession._active_s8s_session_id is not None

    get_session_request = GetSessionRequest()
    get_session_request.name = first_session_name
    session = session_controller_client.get_session(get_session_request)

    assert session.state in [
        Session.State.TERMINATING,
        Session.State.TERMINATED,
    ]

    get_session_request = GetSessionRequest()
    get_session_request.name = second_session_name
    session = session_controller_client.get_session(get_session_request)

    assert session.state == Session.State.ACTIVE
    connect_session.stop()


@pytest.fixture
def session_template_name(
    image_version,
    test_project,
    test_region,
    test_subnetwork_uri,
    session_template_controller_client,
):
    create_session_template_request = CreateSessionTemplateRequest()
    create_session_template_request.parent = (
        f"projects/{test_project}/locations/{test_region}"
    )
    session_template = SessionTemplate()
    session_template.environment_config.execution_config.subnetwork_uri = (
        test_subnetwork_uri
    )
    if image_version:
        session_template.runtime_config.version = image_version
    session_template_name = f"projects/{test_project}/locations/{test_region}/sessionTemplates/spark-connect-test-template-{uuid.uuid4().hex[0:12]}"
    session_template.name = session_template_name
    create_session_template_request.session_template = session_template
    session_template_controller_client.create_session_template(
        create_session_template_request
    )
    get_session_template_request = GetSessionTemplateRequest()
    get_session_template_request.name = session_template_name
    session_template = session_template_controller_client.get_session_template(
        get_session_template_request
    )
    assert (
        session_template.runtime_config.version == image_version
        if image_version
        else DataprocSparkSession._DEFAULT_RUNTIME_VERSION
    )

    yield session_template.name
    delete_session_template_request = DeleteSessionTemplateRequest()
    delete_session_template_request.name = session_template_name
    session_template_controller_client.delete_session_template(
        delete_session_template_request
    )


def test_create_spark_session_with_session_template_and_user_provided_dataproc_config(
    image_version,
    test_project,
    test_region,
    session_template_name,
    session_controller_client,
):
    """Test creating a Spark session with a session template and user-provided Dataproc configuration."""
    dataproc_config = Session()
    dataproc_config.environment_config.execution_config.ttl = {"seconds": 64800}
    dataproc_config.session_template = session_template_name
    connect_session = (
        DataprocSparkSession.builder.config("spark.executor.cores", "7")
        .dataprocSessionConfig(dataproc_config)
        .config("spark.executor.cores", "16")
        .getOrCreate()
    )
    session_name = f"projects/{test_project}/locations/{test_region}/sessions/{DataprocSparkSession._active_s8s_session_id}"

    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)

    assert session.state == Session.State.ACTIVE
    assert session.session_template == session_template_name
    assert (
        session.environment_config.execution_config.ttl
        == datetime.timedelta(seconds=64800)
    )
    assert (
        session.runtime_config.properties["spark:spark.executor.cores"] == "16"
    )
    assert DataprocSparkSession._active_s8s_session_uuid is not None

    connect_session.stop()
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)

    assert session.state in [
        Session.State.TERMINATING,
        Session.State.TERMINATED,
    ]
    assert DataprocSparkSession._active_s8s_session_uuid is None


def test_add_artifacts_pypi_package():
    """Test adding PyPI packages as artifacts to a Spark session."""
    connect_session = DataprocSparkSession.builder.getOrCreate()
    from pyspark.sql.connect.functions import udf, sum
    from pyspark.sql.types import IntegerType

    def generate_random2(row) -> int:
        import random2 as random

        return row + random.Random().randint(1, 5)

    connect_session.addArtifacts("random2", pypi=True)

    # Force evaluation of udf using random2 on workers
    sum_random = (
        connect_session.range(1, 10)
        .withColumn(
            "anotherCol", udf(generate_random2)("id").cast(IntegerType())
        )
        .select(sum("anotherCol"))
        .collect()[0][0]
    )

    assert isinstance(sum_random, int), "Result is not of type int"
    connect_session.stop()


def test_sql_functions(connect_session):
    """Test basic SQL functions like col(), sum(), count(), etc."""
    # Create a test DataFrame
    df = connect_session.createDataFrame(
        [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 150)],
        ["id", "name", "amount"],
    )

    # Test col() function
    result_col = df.select(F.col("name")).collect()
    assert len(result_col) == 3
    assert result_col[0]["name"] == "Alice"

    # Test aggregation functions
    sum_result = df.select(F.sum("amount")).collect()[0][0]
    assert sum_result == 450

    count_result = df.select(F.count("id")).collect()[0][0]
    assert count_result == 3

    # Test with where clause using col()
    filtered_df = df.where(F.col("amount") > 150)
    filtered_count = filtered_df.count()
    assert filtered_count == 1

    # Test multiple column operations
    df_with_calc = df.select(
        F.col("id"),
        F.col("name"),
        F.col("amount"),
        (F.col("amount") * 0.1).alias("tax"),
    )
    tax_results = df_with_calc.collect()
    assert tax_results[0]["tax"] == 10.0
    assert tax_results[1]["tax"] == 20.0
    assert tax_results[2]["tax"] == 15.0


def test_sql_udf(connect_session):
    """Test SQL UDF registration and usage."""
    # Create a test DataFrame
    df = connect_session.createDataFrame(
        [(1, "hello"), (2, "world"), (3, "spark")], ["id", "text"]
    )

    # Register DataFrame for SQL queries
    df.createOrReplaceTempView("test_table")

    # Define and register a Python UDF
    def uppercase_func(text):
        return text.upper() if text else None

    # Test UDF with DataFrame API
    uppercase_udf = F.udf(uppercase_func, StringType())
    df_with_udf = df.select(
        "id", "text", uppercase_udf(F.col("text")).alias("upper_text")
    )
    df_result = df_with_udf.collect()

    assert df_result[0]["upper_text"] == "HELLO"
    assert df_result[1]["upper_text"] == "WORLD"

    # Clean up
    connect_session.sql("DROP VIEW IF EXISTS test_table")
