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
#
# Copyright (c) 2025 pandas-gbq Authors
# All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import json
import importlib
import logging
from pathlib import Path
from typing import Callable, Tuple, List

from google.cloud.dataproc_spark_connect.constants import (
    CLIENT_LABEL_VALUE_UNKNOWN,
    CLIENT_LABEL_VALUE_COLAB,
    CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
    CLIENT_LABEL_VALUE_BQ_STUDIO,
    CLIENT_LABEL_VALUE_VSCODE,
    CLIENT_LABEL_VALUE_JUPYTER,
    CLIENT_LABEL_VALUE_WORKBENCH,
    CLIENT_LABEL_VALUE_INTELLIJ,
    CLIENT_LABEL_VALUE_PYCHARM,
)

# Extension & plugin identifiers
GOOGLE_CLOUD_CODE_EXTENSION_NAME = "googlecloudtools.cloudcode"
BIGQUERY_JUPYTER_PLUGIN_NAME = "bigquery_jupyter_plugin"
DATAPROC_JUPYTER_PLUGIN_NAME = "google.cloud.dataproc_spark_connect"


def _is_vscode_extension_installed(extension_id: str) -> bool:
    """
    Checks if a given Visual Studio Code extension is installed.

    Args:
        extension_id: The ID of the extension.
    Returns:
        True if installed, False otherwise.
    """
    try:
        vscode_dir = Path.home() / ".vscode" / "extensions"
        if not vscode_dir.exists():
            return False
        for item in vscode_dir.iterdir():
            if not item.is_dir() or not item.name.startswith(
                extension_id + "-"
            ):
                continue
            manifest = item / "package.json"
            if manifest.is_file():
                json.load(manifest.open(encoding="utf-8"))
                return True
    except Exception:
        pass
    return False


def _is_package_installed(package_name: str) -> bool:
    """
    Checks if a Python package is importable.

    Args:
        package_name: Name of the package.
    Returns:
        True if import succeeds, False otherwise.
    """
    try:
        importlib.import_module(package_name)
        return True
    except ImportError:
        return False


def is_vscode() -> bool:
    """True if running in VS Code."""
    return os.getenv("VSCODE_PID") is not None


def is_vscode_cloud_code() -> bool:
    """True if Cloud Code extension is present in VS Code."""
    return _is_vscode_extension_installed(GOOGLE_CLOUD_CODE_EXTENSION_NAME)


def is_jupyter() -> bool:
    """True if running in a Jupyter environment."""
    return os.getenv("JPY_PARENT_PID") is not None


def is_jupyter_bigquery_plugin_installed() -> bool:
    """True if BigQuery JupyterLab plugin is installed."""
    return _is_package_installed(BIGQUERY_JUPYTER_PLUGIN_NAME)


def is_jupyter_dataproc_plugin_installed() -> bool:
    """True if Dataproc Spark Connect Jupyter plugin is installed."""
    return _is_package_installed(DATAPROC_JUPYTER_PLUGIN_NAME)


def is_bq_studio() -> bool:
    """
    True if BigQuery Studio is the default Dataproc Spark Connect datasource.
    """
    return os.getenv("DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE") == "bigquery"


def is_colab() -> bool:
    """True if running in Google Colab."""
    return os.getenv("COLAB_RELEASE_TAG") is not None


def is_colab_enterprise() -> bool:
    """True if running in Colab Enterprise (Vertex AI)."""
    return os.getenv("VERTEX_PRODUCT") == "COLAB_ENTERPRISE"


def is_workbench() -> bool:
    """True if running in AI Workbench (managed Jupyter)."""
    return os.getenv("VERTEX_PRODUCT") == "WORKBENCH_INSTANCE"


def is_intellij() -> bool:
    """True if running inside IntelliJ IDEA environment."""
    return os.getenv("IDEA_INITIAL_DIRECTORY") is not None


def is_pycharm() -> bool:
    """True if running inside PyCharm IDE."""
    return os.getenv("PYCHARM_HOSTED") is not None


def get_client_environment_label() -> str:
    """
    Map current environment to a standardized client label.

    Priority order:
      Colab Enterprise → Colab → Workbench-Jupyter → BQ Studio
      → VSCode → IntelliJ → PyCharm → Jupyter → Unknown
    """
    checks: List[Tuple[Callable[[], bool], str]] = [
        (is_colab_enterprise, CLIENT_LABEL_VALUE_COLAB_ENTERPRISE),
        (is_colab, CLIENT_LABEL_VALUE_COLAB),
        (is_workbench, CLIENT_LABEL_VALUE_WORKBENCH),
        (is_bq_studio, CLIENT_LABEL_VALUE_BQ_STUDIO),
        (is_vscode, CLIENT_LABEL_VALUE_VSCODE),
        (is_intellij, CLIENT_LABEL_VALUE_INTELLIJ),
        (is_pycharm, CLIENT_LABEL_VALUE_PYCHARM),
        (is_jupyter, CLIENT_LABEL_VALUE_JUPYTER),
    ]

    for detector, label in checks:
        try:
            if detector():
                return label
        except Exception:
            pass
    return CLIENT_LABEL_VALUE_UNKNOWN
