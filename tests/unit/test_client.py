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
from unittest.mock import MagicMock, patch
from pyspark.sql.connect.proto import ExecutePlanRequest, UserContext
from copy import deepcopy

from google.cloud.dataproc_spark_connect.client import DataprocSparkConnectClient


class DataprocSparkConnectClientTest(unittest.TestCase):

    @patch(
        "google.cloud.dataproc_spark_connect.client.core.DataprocSparkConnectClient._generate_dataproc_operation_id"
    )
    @patch(
        "pyspark.sql.connect.client.SparkConnectClient._execute_plan_request_with_metadata"
    )
    @patch(
        "pyspark.sql.connect.client.SparkConnectClient.__init__",
        return_value=None,
    )
    def test_execute_plan_request_default_behaviour(
        self,
        mock_super_init,
        mock_super_execute_plan_request,
        mock_generate_dataproc_operation_id,
    ):
        test_uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        test_execute_plan_request: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=None,
        )
        mock_super_execute_plan_request.return_value = deepcopy(
            test_execute_plan_request
        )
        mock_generate_dataproc_operation_id.return_value = test_uuid

        client = DataprocSparkConnectClient()

        self.assertIsNone(client.latest_operation_id)

        result_request = client._execute_plan_request_with_metadata()
        self.assertEqual(client.latest_operation_id, test_uuid)
        self.assertEqual(result_request.operation_id, test_uuid)

        mock_super_execute_plan_request.assert_called_once()
        mock_generate_dataproc_operation_id.assert_called_once()
        self.assertEqual(
            result_request.session_id, test_execute_plan_request.session_id
        )
        self.assertEqual(
            result_request.client_type, test_execute_plan_request.client_type
        )
        self.assertEqual(result_request.tags, test_execute_plan_request.tags)
        self.assertEqual(
            result_request.user_context.user_id,
            test_execute_plan_request.user_context.user_id,
        )

    @patch(
        "google.cloud.dataproc_spark_connect.client.core.DataprocSparkConnectClient._generate_dataproc_operation_id"
    )
    @patch(
        "pyspark.sql.connect.client.SparkConnectClient._execute_plan_request_with_metadata"
    )
    @patch(
        "pyspark.sql.connect.client.SparkConnectClient.__init__",
        return_value=None,
    )
    def test_execute_plan_request_with_operation_id_provided(
        self,
        mock_super_init,
        mock_super_execute_plan_request,
        mock_generate_dataproc_operation_id,
    ):
        test_uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        test_execute_plan_request: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id="d27f4fc9-f627-4b72-b20a-aebb2481df74",
        )
        mock_super_execute_plan_request.return_value = deepcopy(
            test_execute_plan_request
        )
        mock_generate_dataproc_operation_id.return_value = test_uuid

        client = DataprocSparkConnectClient()

        self.assertIsNone(client.latest_operation_id)

        result_request = client._execute_plan_request_with_metadata()
        self.assertEqual(
            client.latest_operation_id, test_execute_plan_request.operation_id
        )

        mock_super_execute_plan_request.assert_called_once()
        mock_generate_dataproc_operation_id.assert_not_called()
        self.assertEqual(
            result_request.operation_id, test_execute_plan_request.operation_id
        )
        self.assertEqual(
            result_request.session_id, test_execute_plan_request.session_id
        )
        self.assertEqual(
            result_request.client_type, test_execute_plan_request.client_type
        )
        self.assertEqual(result_request.tags, test_execute_plan_request.tags)
        self.assertEqual(
            result_request.user_context.user_id,
            test_execute_plan_request.user_context.user_id,
        )
