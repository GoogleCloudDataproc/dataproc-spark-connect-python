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
from unittest import mock

from google.api_core.exceptions import (
    FailedPrecondition,
    InvalidArgument,
    NotFound,
)
from google.cloud.dataproc_v1 import (
    CreateSessionRequest,
    GetSessionRequest,
    Session,
    SessionTemplate,
    TerminateSessionRequest,
)
from google.protobuf import text_format
from google.protobuf.text_format import ParseError
from pyspark.sql.connect.client.core import ConfigResult
from pyspark.sql.connect.proto import ConfigResponse

from google.cloud.dataproc_spark_connect import DataprocSparkSession


class DataprocRemoteSparkSessionBuilderTests(unittest.TestCase):

    def setUp(self):
        self._default_runtime_version = "3.0"
        self._resources_dir = os.path.join(
            os.path.dirname(__file__), "resources"
        )
        self.original_environment = dict(os.environ)
        os.environ["DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG"] = (
            self._resources_dir + "/session.textproto"
        )
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
        os.environ["GOOGLE_CLOUD_REGION"] = "test-region"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environment)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    def test_create_spark_session_with_default_notebook_behavior(
        self,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
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
            "Spark Connect Server": "https://spark-connect-server/"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        create_session_request = CreateSessionRequest()
        create_session_request = CreateSessionRequest.wrap(
            text_format.Parse(
                """
        parent: "projects/test-project/regions/test-region"
        session {
          name: "projects/test-project/regions/test-region/sessions/sc-20240702-103952-abcdef"
          runtime_config {
            version: "_DEFAULT_RUNTIME_VERSION_"
          }
          environment_config {
            execution_config {
              network_uri: "test_network_uri"
              idle_ttl {
                seconds: 5
              }
            }
          }
          spark_connect_session {
          }
          spark {
          }
        }
        session_id: "sc-20240702-103952-abcdef"
        """.replace(
                    "_DEFAULT_RUNTIME_VERSION_", self._default_runtime_version
                ),
                CreateSessionRequest.pb(create_session_request),
            )
        )
        try:
            session = DataprocSparkSession.builder.getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            session_response = Session()
            session_response.state = Session.State.TERMINATING
            mock_session_controller_client_instance.get_session.return_value = (
                session_response
            )
            if session is not None:
                session.stop()
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
    def test_create_session_with_user_provided_dataproc_config(
        self,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
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
            "Spark Connect Server": "https://spark-connect-server/"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        create_session_request = CreateSessionRequest()
        create_session_request = CreateSessionRequest.wrap(
            text_format.Parse(
                """
        parent: "projects/test-project/regions/test-region"
        session {
          name: "projects/test-project/regions/test-region/sessions/sc-20240702-103952-abcdef"
          runtime_config {
            version: "_DEFAULT_RUNTIME_VERSION_"
            properties: {
                key: "spark.executor.cores"
                value: "16"
            }
          }
          environment_config {
            execution_config {
              network_uri: "user_passed_network_uri"
              ttl {
                seconds: 10
              }
            }
          }
          spark_connect_session {
          }
          spark {
          }
        }
        session_id: "sc-20240702-103952-abcdef"
        """.replace(
                    "_DEFAULT_RUNTIME_VERSION_", self._default_runtime_version
                ),
                CreateSessionRequest.pb(create_session_request),
            )
        )
        try:
            dataproc_config = Session()
            dataproc_config.environment_config.execution_config.network_uri = (
                "user_passed_network_uri"
            )
            dataproc_config.environment_config.execution_config.ttl = {
                "seconds": 10
            }
            dataproc_config.runtime_config.properties = {
                "spark.executor.cores": "8"
            }
            session = (
                DataprocSparkSession.builder.config("spark.executor.cores", "6")
                .dataprocConfig(dataproc_config)
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
            session_response = Session()
            session_response.state = Session.State.TERMINATING
            mock_session_controller_client_instance.get_session.return_value = (
                session_response
            )
            if session is not None:
                session.stop()
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

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    def test_create_session_with_session_template(
        self,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
        mock_session_template_controller_client,
    ):
        session = None
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
            "Spark Connect Server": "https://spark-connect-server/"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        mock_session_template_controller_client_instance = (
            mock_session_template_controller_client.return_value
        )
        session_template_response = SessionTemplate()
        session_template_response.name = "projects/test-project/locations/test-region/sessionTemplates/test_template"
        session_template_response.environment_config.execution_config.network_uri = (
            "template_network_uri"
        )
        mock_session_template_controller_client_instance.get_session_template.return_value = (
            session_template_response
        )
        create_session_request = CreateSessionRequest()
        create_session_request = CreateSessionRequest.wrap(
            text_format.Parse(
                """
        parent: "projects/test-project/regions/test-region"
        session {
          name: "projects/test-project/regions/test-region/sessions/sc-20240702-103952-abcdef"
          runtime_config {
            version: "_DEFAULT_RUNTIME_VERSION_"
          }
          spark_connect_session {
          }
          spark {
          }
          session_template: "projects/test-project/locations/test-region/sessionTemplates/test_template"
        }
        session_id: "sc-20240702-103952-abcdef"
        """.replace(
                    "_DEFAULT_RUNTIME_VERSION_", self._default_runtime_version
                ),
                CreateSessionRequest.pb(create_session_request),
            )
        )
        try:
            dataproc_config = Session()
            dataproc_config.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
            session = DataprocSparkSession.builder.dataprocConfig(
                dataproc_config
            ).getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            session_response = Session()
            session_response.state = Session.State.TERMINATING
            mock_session_controller_client_instance.get_session.return_value = (
                session_response
            )
            if session is not None:
                session.stop()
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

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    def test_create_session_with_user_provided_dataproc_config_and_session_template(
        self,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
        mock_session_template_controller_client,
    ):
        session = None
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
            "Spark Connect Server": "https://spark-connect-server/"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        mock_session_template_controller_client_instance = (
            mock_session_template_controller_client.return_value
        )
        session_template_response = SessionTemplate()
        session_template_response.name = "projects/test-project/locations/test-region/sessionTemplates/test_template"
        session_template_response.environment_config.execution_config.network_uri = (
            "template_network_uri"
        )
        mock_session_template_controller_client_instance.get_session_template.return_value = (
            session_template_response
        )
        create_session_request = CreateSessionRequest()
        create_session_request = CreateSessionRequest.wrap(
            text_format.Parse(
                """
        parent: "projects/test-project/regions/test-region"
        session {
          name: "projects/test-project/regions/test-region/sessions/sc-20240702-103952-abcdef"
          runtime_config {
            version: "_DEFAULT_RUNTIME_VERSION_"
          }
          environment_config {
            execution_config {
              ttl {
                seconds: 10
              }
            }
          }
          spark_connect_session {
          }
          spark {
          }
          session_template: "projects/test-project/locations/test-region/sessionTemplates/test_template"
        }
        session_id: "sc-20240702-103952-abcdef"
        """.replace(
                    "_DEFAULT_RUNTIME_VERSION_", self._default_runtime_version
                ),
                CreateSessionRequest.pb(create_session_request),
            )
        )
        try:
            dataproc_config = Session()
            dataproc_config.environment_config.execution_config.ttl = {
                "seconds": 10
            }
            dataproc_config.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
            session = DataprocSparkSession.builder.dataprocConfig(
                dataproc_config
            ).getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            session_response = Session()
            session_response.state = Session.State.TERMINATING
            mock_session_controller_client_instance.get_session.return_value = (
                session_response
            )
            if session is not None:
                session.stop()
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

    def test_create_spark_session_with_incorrect_dataproc_config_file_path(
        self,
    ):
        with self.assertRaises(FileNotFoundError) as e:
            with mock.patch.dict(
                "os.environ",
                {
                    "DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG": self._resources_dir
                    + "/session1.textproto",
                },
            ):
                DataprocSparkSession.builder.getOrCreate()
        self.assertEqual(
            e.exception.args[0],
            f"File '{self._resources_dir}/session1.textproto' not found",
        )

    def test_create_spark_session_with_invalid_dataproc_config_file(self):
        with self.assertRaises(ParseError):
            with mock.patch.dict(
                "os.environ",
                {
                    "DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG": self._resources_dir
                    + "/parse_error.textproto",
                },
            ):
                DataprocSparkSession.builder.getOrCreate()

    def test_create_spark_session_unsupported_dataproc_config_version(self):
        with self.assertRaises(ValueError) as e:
            with mock.patch.dict(
                "os.environ",
                {
                    "DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG": self._resources_dir
                    + "/unsupported_version.textproto",
                },
            ):
                DataprocSparkSession.builder.getOrCreate()
        self.assertTrue(
            e.exception.args[0].startswith(
                "runtime_config.version 2.1 is not supported"
            )
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
            DataprocSparkSession.builder.dataprocConfig(Session()).getOrCreate()
        self.assertEqual(
            e.exception.args[0],
            "Error while creating serverless session "
            "https://console.cloud.google.com/dataproc/interactive/test-region/sc-20240702-103952-abcdef : "
            "Testing create session failure",
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
        with self.assertRaises(RuntimeError) as e:
            DataprocSparkSession.builder.dataprocConfig(Session()).getOrCreate()
            self.assertEqual(
                e.exception.args[0],
                "Error while creating serverless session: "
                "400 Network does not have permissions",
            )

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_stop_spark_session_with_terminated_s8s_session(
        self,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "https://spark-connect-server/"
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
    def test_stop_spark_session_with_deleted_s8s_session(
        self,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "https://spark-connect-server/"
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
    def test_stop_spark_session_wait_for_terminating_state(
        self,
        mock_dataproc_session_id,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "https://spark-connect-server/"
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


if __name__ == "__main__":
    unittest.main()
