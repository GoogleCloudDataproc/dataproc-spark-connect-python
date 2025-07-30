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

from google.cloud.dataproc_spark_connect.session import DataprocSparkSession


class TestPythonVersionCheck(unittest.TestCase):

    def test_python_version_mismatch_warning_for_runtime_12(self):
        """Test that warning is shown when client Python doesn't match runtime 1.2 (Python 3.12)"""
        with mock.patch("sys.version_info", (3, 11, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("1.2")

                expected_warning = (
                    "Python version mismatch detected: Client is using Python 3.11, "
                    "but Dataproc runtime 1.2 uses Python 3.12. "
                    "This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    "Consider using Python 3.12 for optimal UDF execution."
                )
                mock_warn.assert_called_once_with(
                    expected_warning, stacklevel=3
                )

    def test_python_version_mismatch_warning_for_runtime_22(self):
        """Test that warning is shown when client Python doesn't match runtime 2.2 (Python 3.12)"""
        with mock.patch("sys.version_info", (3, 11, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("2.2")

                expected_warning = (
                    "Python version mismatch detected: Client is using Python 3.11, "
                    "but Dataproc runtime 2.2 uses Python 3.12. "
                    "This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    "Consider using Python 3.12 for optimal UDF execution."
                )
                mock_warn.assert_called_once_with(
                    expected_warning, stacklevel=3
                )

    def test_python_version_mismatch_warning_for_runtime_23(self):
        """Test that warning is shown when client Python doesn't match runtime 2.3 (Python 3.11)"""
        with mock.patch("sys.version_info", (3, 12, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("2.3")

                expected_warning = (
                    "Python version mismatch detected: Client is using Python 3.12, "
                    "but Dataproc runtime 2.3 uses Python 3.11. "
                    "This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    "Consider using Python 3.11 for optimal UDF execution."
                )
                mock_warn.assert_called_once_with(
                    expected_warning, stacklevel=3
                )

    def test_no_warning_when_python_versions_match_runtime_12(self):
        """Test that no warning is shown when client Python matches runtime 1.2 (Python 3.12)"""
        with mock.patch("sys.version_info", (3, 12, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("1.2")

                mock_warn.assert_not_called()

    def test_no_warning_when_python_versions_match_runtime_22(self):
        """Test that no warning is shown when client Python matches runtime 2.2 (Python 3.12)"""
        with mock.patch("sys.version_info", (3, 12, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("2.2")

                mock_warn.assert_not_called()

    def test_no_warning_when_python_versions_match_runtime_23(self):
        """Test that no warning is shown when client Python matches runtime 2.3 (Python 3.11)"""
        with mock.patch("sys.version_info", (3, 11, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("2.3")

                mock_warn.assert_not_called()

    def test_no_warning_for_unknown_runtime_version(self):
        """Test that no warning is shown for unknown runtime versions"""
        with mock.patch("sys.version_info", (3, 10, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = DataprocSparkSession.Builder()
                session_builder._check_python_version_compatibility("unknown")

                mock_warn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
