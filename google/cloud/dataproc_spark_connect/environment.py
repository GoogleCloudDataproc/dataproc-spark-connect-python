# Copyright 2024 Google LLC
#
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
# environment_utils.py

# environment_utils.py

import os
import json
import importlib
import logging
from typing import Callable, Tuple, List

from google.cloud.dataproc_spark_connect.constants import (
    CLIENT_LABEL_VALUE_UNKNOWN,
    CLIENT_LABEL_VALUE_COLAB,
    CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
    CLIENT_LABEL_VALUE_WORKBENCH,
    CLIENT_LABEL_VALUE_BQ_STUDIO,
    CLIENT_LABEL_VALUE_VSCODE,
    CLIENT_LABEL_VALUE_JUPYTER,
)

# Known extension identifier
GOOGLE_CLOUD_CODE_EXTENSION = "googlecloudtools.cloudcode"


def _is_vscode_extension_installed(extension_id: str) -> bool:
    """
    Scan the VS Code extensions
    folder for a given ID.
    """
    try:
        home = os.path.expanduser("~")
        ext_dir = os.path.join(home, ".vscode", "extensions")
        if not os.path.isdir(ext_dir):
            return False

        for folder in os.listdir(ext_dir):
            if not folder.startswith(extension_id + "-"):
                continue
            manifest = os.path.join(ext_dir, folder, "package.json")
            if os.path.isfile(manifest):
                with open(manifest, encoding="utf-8") as f:
                    json.load(f)
                return True
    except Exception:
        logging.exception(
            "Failed to detect extension %s",
            extension_id,
        )
    return False


def _is_package_installed(pkg: str) -> bool:
    """
    Try importing a pkg to
    determine if installed.
    """
    try:
        importlib.import_module(pkg)
        return True
    except ImportError:
        return False


def is_vscode() -> bool:
    """True if running in VS Code."""
    return bool(os.environ.get("VSCODE_PID"))


def is_vscode_cloud_code() -> bool:
    """True if Cloud Code extension present."""
    return _is_vscode_extension_installed(GOOGLE_CLOUD_CODE_EXTENSION)


def is_jupyter() -> bool:
    """True if in a Jupyter env."""
    return bool(os.environ.get("JPY_PARENT_PID"))


def is_bq_studio() -> bool:
    """
    True if BigQuery Jupyter
    plugin is the default datasource.
    """
    return (
        os.environ.get("DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE")
        == "bigquery"
    )


def is_colab() -> bool:
    """True if running in Colab."""
    return bool(os.environ.get("COLAB_RELEASE_TAG"))


def is_colab_enterprise() -> bool:
    """True if deployed via Colab Enterprise."""
    return os.environ.get("VERTEX_PRODUCT") == "COLAB_ENTERPRISE"


def is_workbench() -> bool:
    """True if deployed via Workbench."""
    return os.environ.get("VERTEX_PRODUCT") == "WORKBENCH_INSTANCE"


def get_client_environment_label() -> str:
    """
    Map current environment to
    a standardized client label.
    Priority:
      enterprise → colab →
      workbench → bq_studio →
      vscode → jupyter → unknown
    """
    checks: List[Tuple[Callable[[], bool], str]] = [
        (is_colab_enterprise, CLIENT_LABEL_VALUE_COLAB_ENTERPRISE),
        (is_colab, CLIENT_LABEL_VALUE_COLAB),
        (is_workbench, CLIENT_LABEL_VALUE_WORKBENCH),
        (is_bq_studio, CLIENT_LABEL_VALUE_BQ_STUDIO),
        (is_vscode, CLIENT_LABEL_VALUE_VSCODE),
        (is_jupyter, CLIENT_LABEL_VALUE_JUPYTER),
    ]

    for detector, label in checks:
        if detector():
            return label

    return CLIENT_LABEL_VALUE_UNKNOWN
