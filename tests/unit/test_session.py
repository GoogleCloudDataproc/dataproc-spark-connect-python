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
import os
import unittest

from google.api_core.exceptions import (
    Aborted,
    FailedPrecondition,
    InvalidArgument,
    NotFound,
)
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_spark_connect.exceptions import DataprocSparkConnectException
from google.cloud.dataproc_v1 import (
    AuthenticationConfig,
    CreateSessionRequest,
    GetSessionRequest,
    Session,
    SparkConnectConfig,
    TerminateSessionRequest,
)
from pyspark.sql.connect.client.core import ConfigResult
from pyspark.sql.connect.proto import ConfigResponse
from unittest import mock


class DataprocRemoteSparkSessionBuilderTests(unittest.TestCase):

    def setUp(self):
        self._default_runtime_version = (
            DataprocSparkSession._DEFAULT_RUNTIME_VERSION
        )
        self.original_environment = dict(os.environ)
        os.environ.clear()
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
        os.environ["GOOGLE_CLOUD_REGION"] = "test-region"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environment)

    @staticmethod
    def stopSession(mock_session_controller_client_instance, session):
        session_response = Session()
        session_response.state = Session.State.TERMINATING
        mock_session_controller_client_instance.get_session.return_value = (
            session_response
        )
        if session is not None:
            session.stop()

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_create_spark_session_with_default_notebook_behavior(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"

        try:
            session = DataprocSparkSession.builder.getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_pypi_add_artifacts(
        self,
        mock_session_controller_client,
    ):
        session = None
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            session = DataprocSparkSession.builder.getOrCreate()
            self.assertTrue(isinstance(session, DataprocSparkSession))
            session.addArtifact = mock.MagicMock()

            # Setting two flags together
            with self.assertRaisesRegex(
                ValueError,
                "'pyfile', 'archive', 'file' and/or 'pypi' cannot be True together",
            ):
                session.addArtifacts("abc.txt", file=True, pypi=True)

            # Propagate error
            session.addArtifact.side_effect = Exception("Error installing")
            with self.assertRaisesRegex(
                Exception,
                "Error installing",
            ):
                session.addArtifacts("spacy", pypi=True)
            session.addArtifact.side_effect = None

            # Do install if earlier add artifact resulted in failure
            session.addArtifacts("spacy", pypi=True)
            self.assertEqual(session.addArtifact.call_count, 2)

            # test multiple packages, when already installed
            session.addArtifacts("spacy==1.2.3", "spacy", pypi=True)
            self.assertEqual(session.addArtifact.call_count, 3)
        finally:
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_user_provided_dataproc_config(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.session.environment_config.execution_config.subnetwork_uri = (
            "user_passed_subnetwork_uri"
        )
        create_session_request.session.environment_config.execution_config.ttl = {
            "seconds": 10
        }
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.properties = {
            "spark.executor.cores": "16"
        }
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"

        try:
            dataproc_config = Session()
            dataproc_config.environment_config.execution_config.subnetwork_uri = (
                "user_passed_subnetwork_uri"
            )
            dataproc_config.environment_config.execution_config.ttl = {
                "seconds": 10
            }
            dataproc_config.runtime_config.properties = {
                "spark.executor.cores": "8"
            }
            session = (
                DataprocSparkSession.builder.config("spark.executor.cores", "6")
                .dataprocSessionConfig(dataproc_config)
                .config("spark.executor.cores", "16")
                .getOrCreate()
            )
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_env_vars_config(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        mock.patch.dict(
            os.environ,
            {
                "DATAPROC_SPARK_CONNECT_AUTH_TYPE": "SERVICE_ACCOUNT",
                "DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT": "test-acc@example.com",
                "DATAPROC_SPARK_CONNECT_SUBNET": "test-subnet-from-env",
                "DATAPROC_SPARK_CONNECT_TTL_SECONDS": "12",
                "DATAPROC_SPARK_CONNECT_IDLE_TTL_SECONDS": "89",
            },
        ).start()

        create_session_request = CreateSessionRequest()
        create_session_request.session.environment_config.execution_config.authentication_config.user_workload_authentication_type = (
            AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
        )
        create_session_request.session.environment_config.execution_config.service_account = (
            "test-acc@example.com"
        )
        create_session_request.session.environment_config.execution_config.subnetwork_uri = (
            "test-subnet-from-env"
        )
        create_session_request.session.environment_config.execution_config.ttl = {
            "seconds": 12
        }
        create_session_request.session.environment_config.execution_config.idle_ttl = {
            "seconds": 89
        }
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"

        try:
            session = DataprocSparkSession.builder.getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_session_template(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"

        try:
            dataproc_config = Session()
            dataproc_config.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
            session = DataprocSparkSession.builder.dataprocSessionConfig(
                dataproc_config
            ).getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_user_provided_dataproc_config_and_session_template(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.environment_config.execution_config.ttl = {
            "seconds": 10
        }
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
        create_session_request.session_id = "sc-20240702-103952-abcdef"

        try:
            dataproc_config = Session()
            dataproc_config.environment_config.execution_config.ttl = {
                "seconds": 10
            }
            dataproc_config.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
            session = DataprocSparkSession.builder.dataprocSessionConfig(
                dataproc_config
            ).getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    def test_create_spark_session_with_create_session_failed(
        self,
        mock_dataproc_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_operation = mock.Mock()
        mock_operation.result.side_effect = Exception(
            "Testing create session failure"
        )
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        with self.assertRaises(RuntimeError) as e:
            DataprocSparkSession.builder.dataprocSessionConfig(
                Session()
            ).getOrCreate()
        self.assertEqual(
            "Error while creating Dataproc Session", e.exception.args[0]
        )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_create_spark_session_with_invalid_argument(
        self,
        mock_session_controller_client,
        mock_credentials,
    ):
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_operation = mock.Mock()
        mock_operation.result.side_effect = InvalidArgument(
            "Network does not have permissions"
        )
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        with self.assertRaises(DataprocSparkConnectException) as e:
            DataprocSparkSession.builder.dataprocSessionConfig(
                Session()
            ).getOrCreate()
            self.assertEqual(
                e.exception.error_message,
                "Error while creating Dataproc Session: "
                "400 Network does not have permissions",
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_spark_session_with_inactive_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = False
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"

        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        with self.assertRaises(RuntimeError) as e:
            session = DataprocSparkSession.builder.getOrCreate()
            session.createDataFrame([(1, "Sarah"), (2, "Maria")]).toDF(
                "id", "name"
            ).show()
            self.assertEqual(
                e.exception.args[0],
                "Session not active. Please create a new session ",
            )
        self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_with_terminated_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = DataprocSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.side_effect = FailedPrecondition(
                "Already terminated"
            )
            if session is not None:
                session.stop()
            self.assertIsNone(DataprocSparkSession._active_s8s_session_uuid)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_with_creating_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = DataprocSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.side_effect = Aborted(
                "still being created"
            )
            if session is not None:
                session.stop()
            self.assertIsNone(DataprocSparkSession._active_s8s_session_uuid)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_with_deleted_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = DataprocSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.side_effect = NotFound(
                "Already deleted"
            )
            if session is not None:
                session.stop()
            self.assertIsNone(DataprocSparkSession._active_s8s_session_uuid)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_wait_for_terminating_state(
        self,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = DataprocSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            session_response1 = Session()
            session_response1.state = Session.State.ACTIVE
            session_response2 = Session()
            session_response2.state = Session.State.TERMINATING
            mock_session_controller_client_instance.get_session.side_effect = [
                session_response1,
                session_response2,
            ]
            if session is not None:
                session.stop()
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.get_session.assert_has_calls(
                [mock.call(get_session_request), mock.call(get_session_request)]
            )


    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("google.cloud.dataproc_spark_connect.session.logger") # Mock the logger
    def test_create_session_with_default_datasource_env_var(
        self,
        mock_logger, # Add mock logger parameter
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_dataproc_session_id.return_value = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f" # Use a valid UUID
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f" # Use a valid UUID
        mock_operation.result.side_effect = [session_response, session_response, session_response, session_response] # Provide a response for each getOrCreate call
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        # Scenario 1: DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is not set
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session = DataprocSparkSession.builder.getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[0][0]
            self.assertNotIn("spark.datasource.bigquery.writeMethod", create_session_request.session.runtime_config.properties)
            mock_logger.warning.assert_not_called()
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()


        # Scenario 2: DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is set to "bigquery"
        with mock.patch.dict(os.environ, {"DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE": "bigquery"}, clear=True):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session = DataprocSparkSession.builder.getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[0][0]
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.datasource.bigquery.writeMethod"), "direct")
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.datasource.bigquery.viewsEnabled"), "true")
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.sql.legacy.createHiveTableByDefault"), "false")
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.sql.sources.default"), "bigquery")
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.sql.catalog.spark_catalog"), "com.google.cloud.spark.bigquery.BigLakeMetastoreSparkCatalog")
            mock_logger.warning.assert_not_called()
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

        # Scenario 3: DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value
        with mock.patch.dict(os.environ, {"DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE": "invalid_datasource"}, clear=True):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session = DataprocSparkSession.builder.getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[0][0]
            self.assertNotIn("spark.datasource.bigquery.writeMethod", create_session_request.session.runtime_config.properties)
            mock_logger.warning.assert_called_once_with("DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value: invalid_datasource. Supported value is 'bigquery'.")
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

        # Scenario 4: DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is set to "bigquery" with pre-existing properties
        with mock.patch.dict(os.environ, {"DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE": "bigquery"}, clear=True):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            dataproc_config = Session()
            dataproc_config.runtime_config.properties = {
                "spark.datasource.bigquery.writeMethod": "override_method",
                "spark.some.other.property": "some_value"
            }
            session = DataprocSparkSession.builder.dataprocSessionConfig(dataproc_config).getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[0][0]
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.datasource.bigquery.writeMethod"), "override_method")
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.datasource.bigquery.viewsEnabled"), "true") # Default should still be set
            self.assertEqual(create_session_request.session.runtime_config.properties.get("spark.some.other.property"), "some_value") # Existing property should remain
            mock_logger.warning.assert_not_called()
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

if __name__ == "__main__":
    unittest.main()
