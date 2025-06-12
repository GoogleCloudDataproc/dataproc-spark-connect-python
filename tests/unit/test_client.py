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
from unittest.mock import patch, Mock, MagicMock
from pyspark.sql.connect.proto import ExecutePlanRequest, UserContext
from copy import deepcopy

from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import Session, CreateSessionRequest, SparkConnectConfig
from pyspark.sql.connect.client.core import ConfigResult
from pyspark.sql.connect.proto import ConfigResponse


class DataprocSparkConnectClientTest(unittest.TestCase):

    @staticmethod
    def stopSession(mock_session_controller_client_instance, session):
        session_response = Session()
        session_response.state = Session.State.TERMINATING
        mock_session_controller_client_instance.get_session.return_value = (
            session_response
        )
        if session is not None:
            session.stop()

    @patch("google.auth.default")
    @patch("google.cloud.dataproc_v1.SessionControllerClient")
    @patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession.Builder.generate_dataproc_session_id"
    )
    @patch("google.cloud.dataproc_spark_connect.session.is_s8s_session_active")
    @patch("uuid.uuid4")
    @patch(
        "pyspark.sql.connect.client.SparkConnectClient._execute_plan_request_with_metadata"
    )
    # @patch(
    #     "pyspark.sql.connect.client.SparkConnectClient.__init__",
    #     return_value=None,
    # )
    @patch(
        "google.cloud.dataproc_spark_connect.DataprocSparkSession._display_operation_link"
    )
    def test_execute_plan_request_default_behaviour(
        self,
        mock_display_operation_link,  # to prevent side effects
        mock_super_execute_plan_request,
        mock_uuid4,
        mock_is_s8s_session_active,
        mock_dataproc_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        test_uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        test_execute_plan_request: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=None,
        )

        session = None
        mock_super_execute_plan_request.return_value = deepcopy(
            test_execute_plan_request
        )
        mock_uuid4.return_value = test_uuid
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        mock_dataproc_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = Mock()
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
            DataprocSparkSession._DEFAULT_RUNTIME_VERSION
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"

        try:
            session = DataprocSparkSession.builder.getOrCreate()
            client = session.client

            result_request = client._execute_plan_request_with_metadata()

            self.assertEqual(result_request.operation_id, test_uuid)

            mock_super_execute_plan_request.assert_called_once()
            mock_uuid4.assert_called_once()
            self.assertEqual(
                result_request.session_id, test_execute_plan_request.session_id
            )
            self.assertEqual(
                result_request.client_type,
                test_execute_plan_request.client_type,
            )
            self.assertEqual(
                result_request.tags, test_execute_plan_request.tags
            )
            self.assertEqual(
                result_request.user_context.user_id,
                test_execute_plan_request.user_context.user_id,
            )

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
