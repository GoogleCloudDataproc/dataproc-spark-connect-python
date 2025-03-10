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
import tempfile
import uuid

from google.api_core import client_options

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

import pytest

from google.cloud.spark_connect import GoogleSparkSession


_SERVICE_ACCOUNT_KEY_FILE_ = "service_account_key.json"


@pytest.fixture(params=["2.2", "3.0"])
def image_version(request):
    return request.param


@pytest.fixture
def test_project():
    return os.environ.get("GOOGLE_CLOUD_PROJECT")


@pytest.fixture
def auth_type(request):
    return getattr(request, "param", "SYSTEM_SERVICE_ACCOUNT")


@pytest.fixture
def test_region():
    return os.environ.get("GOOGLE_CLOUD_REGION")


@pytest.fixture
def test_subnet():
    return os.environ.get("GOOGLE_CLOUD_SUBNET")


@pytest.fixture
def test_subnetwork_uri(test_project, test_region, test_subnet):
    return f"projects/{test_project}/regions/{test_region}/subnetworks/{test_subnet}"


@pytest.fixture
def default_config(
    auth_type, image_version, test_project, test_region, test_subnetwork_uri
):
    resources_dir = os.path.join(os.path.dirname(__file__), "resources")
    template_file = os.path.join(resources_dir, "session.textproto")
    with open(template_file) as f:
        template = f.read()
        contents = (
            template.replace("2.2", image_version)
            .replace("subnet-placeholder", test_subnetwork_uri)
            .replace("SYSTEM_SERVICE_ACCOUNT", auth_type)
        )
        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(contents.encode("utf-8"))
            t.close()
            yield t.name
            os.remove(t.name)


@pytest.fixture
def os_environment(default_config, test_project, test_region):
    original_environment = dict(os.environ)
    os.environ["DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG"] = default_config
    if os.path.isfile(_SERVICE_ACCOUNT_KEY_FILE_):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            _SERVICE_ACCOUNT_KEY_FILE_
        )
    yield os.environ
    os.environ.clear()
    os.environ.update(original_environment)


@pytest.fixture
def api_endpoint(test_region):
    return os.environ.get(
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
    return GoogleSparkSession.builder.getOrCreate()


@pytest.fixture
def session_name(test_project, test_region, connect_session):
    return f"projects/{test_project}/locations/{test_region}/sessions/{GoogleSparkSession._active_s8s_session_id}"


@pytest.mark.parametrize("auth_type", ["END_USER_CREDENTIALS"], indirect=True)
def test_create_spark_session_with_default_notebook_behavior(
    auth_type, connect_session, session_name, session_controller_client
):
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)
    assert session.state == Session.State.ACTIVE

    df = connect_session.createDataFrame([(1, "Sarah"), (2, "Maria")]).toDF(
        "id", "name"
    )
    assert str(df) == "DataFrame[id: bigint, name: string]"
    connect_session.sql("DROP TABLE IF EXISTS FOO")
    connect_session.sql(
        """CREATE TABLE FOO (bar long, baz long) USING PARQUET"""
    )
    with pytest.raises(connect_exceptions.AnalysisException) as ex:
        connect_session.sql(
            """CREATE TABLE FOO (bar long, baz long) USING PARQUET"""
        )

        assert "[TABLE_OR_VIEW_ALREADY_EXISTS]" in str(ex)

    assert GoogleSparkSession._active_s8s_session_uuid is not None
    connect_session.sql("DROP TABLE IF EXISTS FOO")
    connect_session.stop()
    session = session_controller_client.get_session(get_session_request)

    assert session.state in [
        Session.State.TERMINATING,
        Session.State.TERMINATED,
    ]
    assert GoogleSparkSession._active_s8s_session_uuid is None


def test_reuse_s8s_spark_session(
    connect_session, session_name, session_controller_client
):
    assert GoogleSparkSession._active_s8s_session_uuid is not None

    first_session_id = GoogleSparkSession._active_s8s_session_id
    first_session_uuid = GoogleSparkSession._active_s8s_session_uuid

    connect_session = GoogleSparkSession.builder.getOrCreate()
    second_session_id = GoogleSparkSession._active_s8s_session_id
    second_session_uuid = GoogleSparkSession._active_s8s_session_uuid

    assert first_session_id == second_session_id
    assert first_session_uuid == second_session_uuid
    assert GoogleSparkSession._active_s8s_session_uuid is not None
    assert GoogleSparkSession._active_s8s_session_id is not None

    connect_session.stop()


def test_stop_spark_session_with_deleted_serverless_session(
    connect_session, session_name, session_controller_client
):
    assert GoogleSparkSession._active_s8s_session_uuid is not None

    delete_session_request = DeleteSessionRequest()
    delete_session_request.name = session_name
    opeation = session_controller_client.delete_session(delete_session_request)
    opeation.result()
    connect_session.stop()

    assert GoogleSparkSession._active_s8s_session_uuid is None
    assert GoogleSparkSession._active_s8s_session_id is None


def test_stop_spark_session_with_terminated_serverless_session(
    connect_session, session_name, session_controller_client
):
    assert GoogleSparkSession._active_s8s_session_uuid is not None

    terminate_session_request = TerminateSessionRequest()
    terminate_session_request.name = session_name
    opeation = session_controller_client.terminate_session(
        terminate_session_request
    )
    opeation.result()
    connect_session.stop()

    assert GoogleSparkSession._active_s8s_session_uuid is None
    assert GoogleSparkSession._active_s8s_session_id is None


def test_get_or_create_spark_session_with_terminated_serverless_session(
    test_project,
    test_region,
    connect_session,
    session_name,
    session_controller_client,
):
    first_session_name = session_name
    second_session_name = None

    assert GoogleSparkSession._active_s8s_session_uuid is not None

    first_session = GoogleSparkSession._active_s8s_session_uuid
    terminate_session_request = TerminateSessionRequest()
    terminate_session_request.name = first_session_name
    opeation = session_controller_client.terminate_session(
        terminate_session_request
    )
    opeation.result()
    connect_session = GoogleSparkSession.builder.getOrCreate()
    second_session = GoogleSparkSession._active_s8s_session_uuid
    second_session_name = f"projects/{test_project}/locations/{test_region}/sessions/{GoogleSparkSession._active_s8s_session_id}"

    assert first_session != second_session
    assert GoogleSparkSession._active_s8s_session_uuid is not None
    assert GoogleSparkSession._active_s8s_session_id is not None

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
    assert session_template.runtime_config.version == image_version

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
    dataproc_config = Session()
    dataproc_config.environment_config.execution_config.ttl = {"seconds": 64800}
    dataproc_config.session_template = session_template_name
    connect_session = (
        GoogleSparkSession.builder.config("spark.executor.cores", "7")
        .googleSessionConfig(dataproc_config)
        .config("spark.executor.cores", "16")
        .getOrCreate()
    )
    session_name = f"projects/{test_project}/locations/{test_region}/sessions/{GoogleSparkSession._active_s8s_session_id}"

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
    assert GoogleSparkSession._active_s8s_session_uuid is not None

    connect_session.stop()
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)

    assert session.state in [
        Session.State.TERMINATING,
        Session.State.TERMINATED,
    ]
    assert GoogleSparkSession._active_s8s_session_uuid is None
