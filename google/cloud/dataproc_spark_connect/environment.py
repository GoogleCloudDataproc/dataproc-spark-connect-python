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

import os
import json
import importlib
import logging
from typing import Optional

from google.cloud.dataproc_spark_connect.constants import (
    CLIENT_LABEL_VALUE_UNKNOWN,
    CLIENT_LABEL_VALUE_COLAB,
    CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
    CLIENT_LABEL_VALUE_WORKBENCH,
    CLIENT_LABEL_VALUE_BQ_STUDIO,
    CLIENT_LABEL_VALUE_VSCODE,
    CLIENT_LABEL_VALUE_JUPYTER,
)

# Known extension and plugin identifiers
GOOGLE_CLOUD_CODE_EXTENSION = "googlecloudtools.cloudcode"
BIGQUERY_JUPYTER_PLUGIN = "bigquery_jupyter_plugin"


def _is_vscode_extension_installed(extension_id: str) -> bool:
    """
        Check if a VS Code extension is installed by scanning
    the user extensions folder.
        :param extension_id: Extension identifier (e.g. 'ms-python.python').
        :return: True if extension folder and valid manifest found.
    """
    try:
        home = os.path.expanduser("~")
        ext_dir = os.path.join(home, ".vscode", "extensions")
        if not os.path.isdir(ext_dir):
            return False

        for d in os.listdir(ext_dir):
            if not d.startswith(extension_id + "-"):
                continue
            manifest = os.path.join(ext_dir, d, "package.json")
            if os.path.isfile(manifest):
                with open(manifest, encoding="utf-8") as f:
                    json.load(f)  # validate JSON
                return True
    except Exception:
        logging.exception(
            "Failed to detect VS Code extension: %s", extension_id
        )
    return False


def _is_package_installed(pkg: str) -> bool:
    """
    Attempt to import a Python package to verify installation.
    :param pkg: Package name (e.g. 'numpy').
    :return: True if import succeeds, False otherwise.
    """
    try:
        importlib.import_module(pkg)
        return True
    except ImportError:
        return False


def is_vscode() -> bool:
    """Return True if running inside VS Code"""
    return bool(os.environ.get("VSCODE_PID"))


def is_vscode_cloud_code() -> bool:
    """Return True if Google Cloud Code extension is installed in VS Code"""
    return _is_vscode_extension_installed(GOOGLE_CLOUD_CODE_EXTENSION)


def is_jupyter() -> bool:
    """Return True if running inside a Jupyter environment"""
    return bool(os.environ.get("JPY_PARENT_PID"))


def is_bigquery_plugin() -> bool:
    """Return True if BigQuery Jupyter plugin is installed"""
    return _is_package_installed(BIGQUERY_JUPYTER_PLUGIN)


def is_colab() -> bool:
    """Return True if running inside Google Colab"""
    return bool(os.environ.get("COLAB_RELEASE_TAG"))


def get_deploy_source() -> Optional[str]:
    """
    Retrieve the deployment source from environment.
    :return: Value of CLOUD_SDK_COMMAND_NAME, or None if unavailable.
    """
    try:
        return os.environ.get("CLOUD_SDK_COMMAND_NAME")
    except Exception:
        logging.warning("Unable to read deploy source", exc_info=True)
        return None


def is_colab_enterprise() -> bool:
    """Return True if deployed via Colab Enterprise"""
    return get_deploy_source() == "notebook_colab_enterprise"


def is_workbench() -> bool:
    """Return True if deployed via Workbench"""
    return get_deploy_source() == "notebook_workbench"


def is_bq_studio() -> bool:
    """Return True if BigQuery Studio is detected"""
    return is_bigquery_plugin()


def get_client_environment_label() -> str:
    """
    Map the current environment to a client label constant.
    Order of checks enforces priority:
    enterprise -> colab -> workbench -> bq_studio -> vscode -> jupyter
    """
    if is_colab_enterprise():
        return CLIENT_LABEL_VALUE_COLAB_ENTERPRISE
    if is_colab():
        return CLIENT_LABEL_VALUE_COLAB
    if is_workbench():
        return CLIENT_LABEL_VALUE_WORKBENCH
    if is_bq_studio():
        return CLIENT_LABEL_VALUE_BQ_STUDIO
    if is_vscode():
        return CLIENT_LABEL_VALUE_VSCODE
    if is_jupyter():
        return CLIENT_LABEL_VALUE_JUPYTER
    return CLIENT_LABEL_VALUE_UNKNOWN
