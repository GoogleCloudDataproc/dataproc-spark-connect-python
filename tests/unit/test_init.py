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
import sys
import unittest
from unittest import mock


class TestPythonVersionCheck(unittest.TestCase):

    def test_python_version_warning_for_unsupported_version(self):
        """Test that warning is shown for unsupported Python versions"""
        # Create a mock version_info object with the necessary attributes
        mock_version_info = mock.MagicMock()
        mock_version_info.__getitem__ = (
            lambda self, key: (3, 10) if key == slice(None, 2) else None
        )
        mock_version_info.major = 3
        mock_version_info.minor = 10
        mock_version_info.micro = 5

        with mock.patch(
            "google.cloud.dataproc_spark_connect.sys.version_info",
            mock_version_info,
        ):
            with mock.patch("warnings.warn") as mock_warn:
                # Clear module cache to force re-import
                if "google.cloud.dataproc_spark_connect" in sys.modules:
                    del sys.modules["google.cloud.dataproc_spark_connect"]

                # Import the module to trigger the version check
                import google.cloud.dataproc_spark_connect

                # Verify warning was called with the expected message
                expected_warning = (
                    "Python version mismatch: Client is using Python 3.10, "
                    "but Dataproc runtime versions support Python 3.11 and 3.12. "
                    "This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    "Consider using a matching Python version for optimal UDF execution."
                )
                mock_warn.assert_any_call(expected_warning)

    def test_python_version_warning_for_python_311(self):
        """Test that informative warning is shown for Python 3.11"""
        # Create a mock version_info object with the necessary attributes
        mock_version_info = mock.MagicMock()
        mock_version_info.__getitem__ = (
            lambda self, key: (3, 11) if key == slice(None, 2) else None
        )
        mock_version_info.major = 3
        mock_version_info.minor = 11
        mock_version_info.micro = 0

        with mock.patch(
            "google.cloud.dataproc_spark_connect.sys.version_info",
            mock_version_info,
        ):
            with mock.patch("warnings.warn") as mock_warn:
                # Clear module cache to force re-import
                if "google.cloud.dataproc_spark_connect" in sys.modules:
                    del sys.modules["google.cloud.dataproc_spark_connect"]

                # Import the module to trigger the version check
                import google.cloud.dataproc_spark_connect

                # Verify warning was called with the expected message
                expected_warning = (
                    "Python 3.11 detected. "
                    "For optimal Python UDF compatibility, use Dataproc runtime version(s): 2.3. "
                    "Using other runtime versions may cause Python UDF execution issues due to version mismatch."
                )
                mock_warn.assert_any_call(expected_warning)

    def test_python_version_warning_for_python_312(self):
        """Test that informative warning is shown for Python 3.12"""
        # Create a mock version_info object with the necessary attributes
        mock_version_info = mock.MagicMock()
        mock_version_info.__getitem__ = (
            lambda self, key: (3, 12) if key == slice(None, 2) else None
        )
        mock_version_info.major = 3
        mock_version_info.minor = 12
        mock_version_info.micro = 0

        with mock.patch(
            "google.cloud.dataproc_spark_connect.sys.version_info",
            mock_version_info,
        ):
            with mock.patch("warnings.warn") as mock_warn:
                # Clear module cache to force re-import
                if "google.cloud.dataproc_spark_connect" in sys.modules:
                    del sys.modules["google.cloud.dataproc_spark_connect"]

                # Import the module to trigger the version check
                import google.cloud.dataproc_spark_connect

                # Verify warning was called with the expected message
                expected_warning = (
                    "Python 3.12 detected. "
                    "For optimal Python UDF compatibility, use Dataproc runtime version(s): 1.2, 2.2. "
                    "Using other runtime versions may cause Python UDF execution issues due to version mismatch."
                )
                mock_warn.assert_any_call(expected_warning)


if __name__ == "__main__":
    unittest.main()
