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


from typing import Optional
import importlib
import json
import os

from google.cloud.dataproc_spark_connect.constants import (
    CLIENT_LABEL_KEY,
    CLIENT_LABEL_VALUE_UNKNOWN,
    CLIENT_LABEL_VALUE_COLAB,
    CLIENT_LABEL_VALUE_COLAB_ENTERPRISE,
    CLIENT_LABEL_VALUE_WORKBENCH,
    CLIENT_LABEL_VALUE_BQ_STUDIO,
    CLIENT_LABEL_VALUE_VSCODE,
    CLIENT_LABEL_VALUE_JUPYTER,
)

# The identifier for GCP VS Code extension
# https://cloud.google.com/code/docs/vscode/install
GOOGLE_CLOUD_CODE_EXTENSION_NAME = "googlecloudtools.cloudcode"


# The identifier for BigQuery Jupyter notebook plugin
# https://cloud.google.com/bigquery/docs/jupyterlab-plugin
BIGQUERY_JUPYTER_PLUGIN_NAME = "bigquery_jupyter_plugin"


def _is_vscode_extension_installed(extension_id: str) -> bool:
    """
    Checks if a given Visual Studio Code extension is installed.
    Args:
        extension_id: The ID of the extension (e.g., "ms-python.python").
    Returns:
        True if the extension is installed, False otherwise.
    """
    try:
        # Determine the user's VS Code extensions directory.
        user_home = os.path.expanduser("~")
        vscode_extensions_dir = os.path.join(user_home, ".vscode", "extensions")

        # Check if the extensions directory exists.
        if os.path.exists(vscode_extensions_dir):
            # Iterate through the subdirectories in the extensions directory.
            for item in os.listdir(vscode_extensions_dir):
                item_path = os.path.join(vscode_extensions_dir, item)
                if os.path.isdir(item_path) and item.startswith(
                    extension_id + "-"
                ):
                    # Check if the folder starts with the extension ID.
                    # Further check for manifest file, as a more robust check.
                    manifest_path = os.path.join(item_path, "package.json")
                    if os.path.exists(manifest_path):
                        try:
                            with open(
                                manifest_path, "r", encoding="utf-8"
                            ) as f:
                                json.load(f)
                            return True
                        except (FileNotFoundError, json.JSONDecodeError):
                            # Corrupted or incomplete extension, or manifest missing.
                            pass
    except Exception:
        logging.exception(
            "An error occurred while checking VS Code extension installation."
        )
        pass

    return False


def _is_package_installed(package_name: str) -> bool:
    """
    Checks if a Python package is installed.
    Args:
        package_name: The name of the package to check (e.g., "requests", "numpy").
    Returns:
        True if the package is installed, False otherwise.
    """
    try:
        importlib.import_module(package_name)
        return True
    except ImportError:
        return False


def is_vscode() -> bool:
    """Checks if the current environment is VS Code."""
    return os.environ.get("VSCODE_PID") is not None


def is_jupyter() -> bool:
    """Checks if the current environment is a Jupyter environment."""
    try:
        return os.environ.get("JPY_PARENT_PID") is not None
    except Exception:
        return False


def is_vscode_google_cloud_code_extension_installed() -> bool:
    """Checks if the Google Cloud Code extension is installed in VS Code."""
    return _is_vscode_extension_installed(GOOGLE_CLOUD_CODE_EXTENSION_NAME)


def is_jupyter_bigquery_plugin_installed() -> bool:
    """Checks if the BigQuery Jupyter plugin is installed."""
    return _is_package_installed(BIGQUERY_JUPYTER_PLUGIN_NAME)


def is_colab() -> bool:
    """Checks if the current environment is Google Colab."""
    try:
        return os.environ.get("COLAB_RELEASE_TAG") is not None
    except Exception:
        return False


def is_colab_enterprise() -> bool:
    return get_deploy_source() == "notebook_colab_enterprise"


def is_workbench_instance() -> bool:
    return get_deploy_source() == "notebook_workbench"


def is_bq_studio() -> bool:
    """
    Determines if the client is BQ Studio based on the presence of the BigQuery
    Jupyter plugin.

    Returns:
        bool: True if the BigQuery Jupyter plugin is installed, indicating
              that the environment is likely BQ Studio; False otherwise.
    """
    return is_jupyter_bigquery_plugin_installed()


def get_client_environment_label() -> str:
    """
    Determines the client environment and returns a corresponding label value.

    Returns:
        str: A string representing the client environment (e.g., "colab", "bq-studio", "vscode", "jupyter", "unknown").
    """
    if is_colab_enterprise():
        return CLIENT_LABEL_VALUE_COLAB_ENTERPRISE
    elif is_colab():
        return CLIENT_LABEL_VALUE_COLAB
    elif is_workbench_instance():
        return CLIENT_LABEL_VALUE_WORKBENCH
    elif is_bq_studio():
        return CLIENT_LABEL_VALUE_BQ_STUDIO
    elif is_vscode():
        return CLIENT_LABEL_VALUE_VSCODE
    elif is_jupyter():  # Generic Jupyter check should be last in the order
        return CLIENT_LABEL_VALUE_JUPYTER
    else:
        return CLIENT_LABEL_VALUE_UNKNOWN


def get_deploy_source() -> Optional[str]:
    """
    Attempts to retrieve the deployment source environment variable, handling potential exceptions.

    Returns:
        Optional[str]: The value of the environment variable if found, otherwise None.
    """
    try:
        return os.environ.get("CLOUD_SDK_COMMAND_NAME")
    except Exception:
        return None
