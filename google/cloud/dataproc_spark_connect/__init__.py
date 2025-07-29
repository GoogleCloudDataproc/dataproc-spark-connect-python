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

# Check Python version and show compatibility warnings for Python UDFs
client_python = sys.version_info[:2]  # (major, minor)

# Runtime version to server Python version mapping
RUNTIME_PYTHON_MAP = {"1.2": (3, 12), "2.2": (3, 12), "2.3": (3, 11)}

# Show informative warnings about Python version mismatches for UDF compatibility
supported_versions = sorted(set(RUNTIME_PYTHON_MAP.values()))
version_strings = [f"{major}.{minor}" for major, minor in supported_versions]

if client_python not in supported_versions:
    warnings.warn(
        f"Python version mismatch: Client is using Python {sys.version_info.major}.{sys.version_info.minor}, "
        f"but Dataproc runtime versions support Python {' and '.join(version_strings)}. "
        f"This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
        f"Consider using a matching Python version for optimal UDF execution."
    )
else:
    # Client has a supported version, inform about runtime compatibility
    matching_runtimes = [
        rt
        for rt, py_ver in RUNTIME_PYTHON_MAP.items()
        if py_ver == client_python
    ]
    runtime_list = ", ".join(matching_runtimes)
    warnings.warn(
        f"Python {client_python[0]}.{client_python[1]} detected. "
        f"For optimal Python UDF compatibility, use Dataproc runtime version(s): {runtime_list}. "
        f"Using other runtime versions may cause Python UDF execution issues due to version mismatch."
    )
