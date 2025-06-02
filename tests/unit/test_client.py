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

from google.cloud.dataproc_spark_connect.client import DataprocSparkConnectClient


class DataprocSparkConnectClientTest(unittest.TestCase):

    def setUp(self):
        self.original_environment = dict(os.environ)
        os.environ.clear()
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
        os.environ["GOOGLE_CLOUD_REGION"] = "test-region"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environment)

    @patch(
        "google.cloud.dataproc_spark_connect.client.core._generate_dataproc_operation_id"
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
        mock_super_execute_plan_request.return_value = MagicMock(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            tags=["mock-tag-from-super"],
            user_context=MagicMock(user_id="mock-user-from-super"),
            operation_id=None,
        )
        mock_generate_dataproc_operation_id.return_value = test_uuid

        client = DataprocSparkConnectClient()

        self.assertIsNone(client.latest_operation_id)

        result_request = client._execute_plan_request_with_metadata()
        self.assertEqual(client.latest_operation_id, test_uuid)
        self.assertEqual(result_request.operation_id, test_uuid)
        self.assertEqual(
            client.latest_operation_id, result_request.operation_id
        )

        mock_super_execute_plan_request.assert_called_once()
        mock_generate_dataproc_operation_id.assert_called_once()
        self.assertEqual(
            result_request.session_id, "mock-session_id-from-super"
        )
        self.assertEqual(
            result_request.client_type, "mock-client_type-from-super"
        )
        self.assertEqual(result_request.tags, ["mock-tag-from-super"])
        self.assertEqual(
            result_request.user_context.user_id, "mock-user-from-super"
        )


if __name__ == "__main__":
    unittest.main()
