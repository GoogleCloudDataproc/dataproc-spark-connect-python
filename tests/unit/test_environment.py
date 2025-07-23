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
        importlib.reload(environment)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environ)

    @mock.patch("os.path.isdir", return_value=True)
    @mock.patch("os.listdir", return_value=["googlecloudtools.cloudcode-1.2.3"])
    @mock.patch("os.path.isfile", return_value=True)
    def test__is_vscode_extension_installed_success(
        self, mock_isfile, mock_listdir, mock_isdir
    ):
        mock_package_json = json.dumps(
            {"publisher": "googlecloudtools", "name": "cloudcode"}
        )
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=mock_package_json)
        ):
            result = environment._is_vscode_extension_installed(
                environment.GOOGLE_CLOUD_CODE_EXTENSION
            )
            self.assertTrue(result)

    @mock.patch("os.path.isdir", return_value=True)
    @mock.patch("os.listdir", return_value=["someotherextension-1.0.0"])
    def test__is_vscode_extension_installed_no_match(
        self, mock_listdir, mock_isdir
    ):
        result = environment._is_vscode_extension_installed(
            environment.GOOGLE_CLOUD_CODE_EXTENSION
        )
        self.assertFalse(result)

    @mock.patch("os.path.isdir", return_value=False)
    def test__is_vscode_extension_installed_no_dir(self, mock_isdir):
        result = environment._is_vscode_extension_installed(
            environment.GOOGLE_CLOUD_CODE_EXTENSION
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
        os.environ.pop("VSCODE_PID", None)
        self.assertFalse(environment.is_vscode())

    def test_is_jupyter_true(self):
        os.environ["JPY_PARENT_PID"] = "67890"
        self.assertTrue(environment.is_jupyter())

    def test_is_jupyter_false(self):
        os.environ.pop("JPY_PARENT_PID", None)
        self.assertFalse(environment.is_jupyter())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment._is_vscode_extension_installed",
        return_value=True,
    )
    def test_is_vscode_cloud_code_true(self, mock_check):
        self.assertTrue(environment.is_vscode_cloud_code())
        mock_check.assert_called_once_with(
            environment.GOOGLE_CLOUD_CODE_EXTENSION
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment._is_vscode_extension_installed",
        return_value=False,
    )
    def test_is_vscode_cloud_code_false(self, mock_check):
        self.assertFalse(environment.is_vscode_cloud_code())

    def test_is_bq_studio_true(self):
        os.environ["DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE"] = "bigquery"
        self.assertTrue(environment.is_bq_studio())

    def test_is_bq_studio_false(self):
        os.environ["DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE"] = "other"
        self.assertFalse(environment.is_bq_studio())
        os.environ.pop("DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE", None)
        self.assertFalse(environment.is_bq_studio())

    def test_is_colab_true(self):
        os.environ["COLAB_RELEASE_TAG"] = "colab-20240718"
        self.assertTrue(environment.is_colab())

    def test_is_colab_false(self):
        os.environ.pop("COLAB_RELEASE_TAG", None)
        self.assertFalse(environment.is_colab())

    def test_is_colab_enterprise_true(self):
        os.environ["VERTEX_PRODUCT"] = "COLAB_ENTERPRISE"
        self.assertTrue(environment.is_colab_enterprise())

    def test_is_colab_enterprise_false(self):
        os.environ["VERTEX_PRODUCT"] = "OTHER"
        self.assertFalse(environment.is_colab_enterprise())

    def test_is_workbench_true(self):
        os.environ["VERTEX_PRODUCT"] = "WORKBENCH_INSTANCE"
        self.assertTrue(environment.is_workbench())

    def test_is_workbench_false(self):
        os.environ["VERTEX_PRODUCT"] = "OTHER"
        self.assertFalse(environment.is_workbench())

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
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
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=True,
    )
    def test_get_client_environment_label_colab(self, *args):
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_COLAB,
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
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
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
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
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
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_bq_studio",
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
        self.assertEqual(
            environment.get_client_environment_label(),
            CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
        )


if __name__ == "__main__":
    unittest.main()
