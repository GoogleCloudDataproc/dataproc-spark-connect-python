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

import json
import os
import unittest
from unittest import mock
import importlib

from google.cloud.dataproc_spark_connect import environment
from google.cloud.dataproc_spark_connect.constants import (
    CLIENT_LABEL_VALUE_BQ_STUDIO,
    CLIENT_LABEL_VALUE_COLAB,
    CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
    CLIENT_LABEL_VALUE_JUPYTER,
    CLIENT_LABEL_VALUE_UNKNOWN,
    CLIENT_LABEL_VALUE_VSCODE,
    CLIENT_LABEL_VALUE_WORKBENCH,
)


class TestEnvironment(unittest.TestCase):

    def setUp(self):
        self.original_environ = os.environ.copy()
        # Reload module to pick up any environment changes
        importlib.reload(environment)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environ)

    @mock.patch("os.path.exists")
    @mock.patch("os.listdir")
    @mock.patch("os.path.isdir")
    def test__is_vscode_extension_installed_success(
        self, mock_isdir, mock_listdir, mock_exists
    ):
        mock_exists.return_value = True
        mock_listdir.return_value = ["googlecloudtools.cloudcode-1.2.3"]
        mock_isdir.return_value = True

        mock_package_json = json.dumps(
            {"publisher": "googlecloudtools", "name": "cloudcode"}
        )
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=mock_package_json)
        ):
            result = environment._is_vscode_extension_installed(
                "googlecloudtools.cloudcode"
            )
            self.assertTrue(result)

    @mock.patch("os.path.exists")
    @mock.patch("os.listdir")
    def test__is_vscode_extension_installed_no_match(
        self, mock_listdir, mock_exists
    ):
        mock_exists.return_value = True
        mock_listdir.return_value = ["someotherextension-1.0.0"]
        result = environment._is_vscode_extension_installed(
            "googlecloudtools.cloudcode"
        )
        self.assertFalse(result)

    @mock.patch("os.path.exists")
    def test__is_vscode_extension_installed_no_dir(self, mock_exists):
        mock_exists.return_value = False
        result = environment._is_vscode_extension_installed(
            "googlecloudtools.cloudcode"
        )
        self.assertFalse(result)

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.importlib.import_module"
    )
    def test__is_package_installed_success(self, mock_import_module):
        mock_import_module.return_value = True
        result = environment._is_package_installed("requests")
        self.assertTrue(result)
        mock_import_module.assert_called_once_with("requests")

    def test_is_vscode_true(self):
        os.environ["VSCODE_PID"] = "12345"
        self.assertTrue(environment.is_vscode())

    def test_is_vscode_false(self):
        if "VSCODE_PID" in os.environ:
            del os.environ["VSCODE_PID"]
        self.assertFalse(environment.is_vscode())

    def test_is_jupyter_true(self):
        os.environ["JPY_PARENT_PID"] = "67890"
        self.assertTrue(environment.is_jupyter())

    def test_is_jupyter_false(self):
        if "JPY_PARENT_PID" in os.environ:
            del os.environ["JPY_PARENT_PID"]
        self.assertFalse(environment.is_jupyter())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment._is_vscode_extension_installed",
        return_value=True,
    )
    def test_is_vscode_google_cloud_code_extension_installed_true(
        self, mock_check
    ):
        self.assertTrue(
            environment.is_vscode_google_cloud_code_extension_installed()
        )
        mock_check.assert_called_once_with(
            environment.GOOGLE_CLOUD_CODE_EXTENSION_NAME
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment._is_vscode_extension_installed",
        return_value=False,
    )
    def test_is_vscode_google_cloud_code_extension_installed_false(
        self, mock_check
    ):
        self.assertFalse(
            environment.is_vscode_google_cloud_code_extension_installed()
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment._is_package_installed",
        return_value=True,
    )
    def test_is_jupyter_bigquery_plugin_installed_true(self, mock_check):
        self.assertTrue(environment.is_jupyter_bigquery_plugin_installed())
        mock_check.assert_called_once_with(
            environment.BIGQUERY_JUPYTER_PLUGIN_NAME
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment._is_package_installed",
        return_value=False,
    )
    def test_is_jupyter_bigquery_plugin_installed_false(self, mock_check):
        self.assertFalse(environment.is_jupyter_bigquery_plugin_installed())

    def test_is_colab_true(self):
        os.environ["COLAB_RELEASE_TAG"] = "colab-20240718"
        self.assertTrue(environment.is_colab())

    def test_is_colab_false(self):
        if "COLAB_RELEASE_TAG" in os.environ:
            del os.environ["COLAB_RELEASE_TAG"]
        self.assertFalse(environment.is_colab())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.get_deploy_source",
        return_value="notebook_colab_enterprise",
    )
    def test_is_colab_enterprise_true(self, mock_get_deploy_source):
        self.assertTrue(environment.is_colab_enterprise())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.get_deploy_source",
        return_value="other_env",
    )
    def test_is_colab_enterprise_false(self, mock_get_deploy_source):
        self.assertFalse(environment.is_colab_enterprise())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.get_deploy_source",
        return_value="notebook_workbench",
    )
    def test_is_workbench_instance_true(self, mock_get_deploy_source):
        self.assertTrue(environment.is_workbench_instance())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.get_deploy_source",
        return_value="other_env",
    )
    def test_is_workbench_instance_false(self, mock_get_deploy_source):
        self.assertFalse(environment.is_workbench_instance())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jupyter_bigquery_plugin_installed",
        return_value=True,
    )
    def test_is_bq_studio_true(self, mock_check):
        self.assertTrue(environment.is_bq_studio())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jupyter_bigquery_plugin_installed",
        return_value=False,
    )
    def test_is_bq_studio_false(self, mock_check):
        self.assertFalse(environment.is_bq_studio())

    def test_get_deploy_source_present(self):
        # FIX: Modify os.environ directly instead of using mock.patch.dict.
        # This avoids issues with modules caching the os.environ object.
        os.environ["CLOUD_SDK_COMMAND_NAME"] = "test_env"
        self.assertEqual(environment.get_deploy_source(), "test_env")

    def test_get_deploy_source_missing(self):
        if "CLOUD_SDK_COMMAND_NAME" in os.environ:
            del os.environ["CLOUD_SDK_COMMAND_NAME"]
        self.assertIsNone(environment.get_deploy_source())

    # FIX: The following tests are refactored to mock the boolean helper functions
    # directly. This makes the tests more robust and independent of the
    # implementation details of the helpers (e.g., which env vars they check).

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench_instance",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_vscode",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jupyter",
        return_value=False,
    )
    def test_get_client_environment_label_unknown(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_UNKNOWN,
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench_instance",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=True,
    )
    def test_get_client_environment_label_colab(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(), CLIENT_LABEL_VALUE_COLAB
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=True,
    )
    def test_get_client_environment_label_colab_enterprise(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench_instance",
        return_value=True,
    )
    def test_get_client_environment_label_workbench(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_WORKBENCH,
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench_instance",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
        return_value=True,
    )
    def test_get_client_environment_label_bq_studio(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_BQ_STUDIO,
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench_instance",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_vscode",
        return_value=True,
    )
    def test_get_client_environment_label_vscode(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_VSCODE,
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench_instance",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_vscode",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jupyter",
        return_value=True,
    )
    def test_get_client_environment_label_jupyter(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_JUPYTER,
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=True,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=True,
    )
    def test_get_client_environment_label_precedence(
        self, mock_colab, mock_colab_ent
    ):
        # Colab Enterprise should take precedence over Colab
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
        )


if __name__ == "__main__":
    unittest.main()
