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
import importlib.metadata
import sys
import warnings

from .session import DataprocSparkSession

old_package_name = "google-spark-connect"
current_package_name = "dataproc-spark-connect"
try:
    importlib.metadata.distribution(old_package_name)
    warnings.warn(
        f"Package '{old_package_name}' is already installed in your environment. "
        f"This might cause conflicts with '{current_package_name}'. "
        f"Consider uninstalling '{old_package_name}' and only install '{current_package_name}'."
    )
except:
    pass

# Check Python version and show compatibility warnings
python_version = sys.version_info[:2]  # (major, minor)

# Runtime version to Python version compatibility mapping
runtime_python_map = {"1.2": (3, 12), "2.2": (3, 12), "2.3": (3, 11)}

# Warn about Python version compatibility
if python_version < (3, 11):
    warnings.warn(
        f"Python 3.11 or higher is required for compatibility. "
        f"You are using Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}. "
        f"Runtime versions have specific Python requirements: "
        f"runtime 1.2/2.2 require Python 3.12, runtime 2.3 requires Python 3.11."
    )
elif python_version == (3, 11):
    # Python 3.11 users should be aware of runtime compatibility
    warnings.warn(
        f"You are using Python 3.11. Note that runtime versions 1.2 and 2.2 require Python 3.12. "
        f"Only runtime version 2.3 is compatible with Python 3.11."
    )
