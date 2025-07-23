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

    def test_python_version_warning_for_old_version(self):
        """Test that warning is shown for Python < 3.11"""
        # Create a mock version_info object with the necessary attributes
        mock_version_info = mock.MagicMock()
        mock_version_info.__getitem__ = (
            lambda self, key: (3, 10) if key == slice(None, 2) else None
        )
        mock_version_info.__lt__ = mock.MagicMock(return_value=True)
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
                    "Python 3.11 or higher is required for compatibility. "
                    "You are using Python 3.10.5. "
                    "Runtime versions have specific Python requirements: "
                    "runtime 1.2/2.2 require Python 3.12, runtime 2.3 requires Python 3.11."
                )
                mock_warn.assert_any_call(expected_warning)

    def test_python_version_warning_for_python_311(self):
        """Test that warning is shown for Python 3.11 about runtime compatibility"""
        # Create a mock version_info object with the necessary attributes
        mock_version_info = mock.MagicMock()
        mock_version_info.__getitem__ = (
            lambda self, key: (3, 11) if key == slice(None, 2) else None
        )
        mock_version_info.__lt__ = mock.MagicMock(return_value=False)
        mock_version_info.__eq__ = lambda self, other: other == (3, 11)
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
                    "You are using Python 3.11. Note that runtime versions 1.2 and 2.2 require Python 3.12. "
                    "Only runtime version 2.3 is compatible with Python 3.11."
                )
                mock_warn.assert_any_call(expected_warning)

    def test_no_python_version_warning_for_python_312_and_higher(self):
        """Test that no warning is shown for Python >= 3.12"""
        # Create a mock version_info object with the necessary attributes
        mock_version_info = mock.MagicMock()
        mock_version_info.__getitem__ = (
            lambda self, key: (3, 12) if key == slice(None, 2) else None
        )
        mock_version_info.__lt__ = mock.MagicMock(return_value=False)
        mock_version_info.__eq__ = lambda self, other: other == (3, 12)
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

                # Check that no Python version warning was called
                # We check for any call containing our specific warning patterns
                python_version_warning_called = False
                for call in mock_warn.call_args_list:
                    if call[0] and (
                        "Python 3.11 or higher is required" in str(call[0][0])
                        or "You are using Python 3.11" in str(call[0][0])
                    ):
                        python_version_warning_called = True
                        break

                self.assertFalse(
                    python_version_warning_called,
                    "Python version warning should not be shown for Python >= 3.12",
                )


if __name__ == "__main__":
    unittest.main()
