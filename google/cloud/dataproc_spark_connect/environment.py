# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import json
import importlib
from pathlib import Path
from typing import Callable, Tuple, List

# Extension & plugin identifiers
GOOGLE_CLOUD_CODE_EXTENSION_NAME = "googlecloudtools.cloudcode"
BIGQUERY_JUPYTER_PLUGIN_NAME = "bigquery_jupyter_plugin"
DATAPROC_JUPYTER_PLUGIN_NAME = "dataproc_jupyter_plugin"


def _is_vscode_extension_installed(extension_id: str) -> bool:
    """Checks if a given VS Code extension (by ID) is installed."""
    try:
        vscode_dir = Path.home() / ".vscode" / "extensions"
        if not vscode_dir.exists():
            return False
        for item in vscode_dir.iterdir():
            if item.is_dir() and item.name.startswith(extension_id + "-"):
                manifest = item / "package.json"
                if manifest.is_file():
                    json.load(manifest.open(encoding="utf-8"))
                    return True
    except Exception:
        pass
    return False


def _is_package_installed(package_name: str) -> bool:
    """True if `import package_name` succeeds."""
    try:
        importlib.import_module(package_name)
        return True
    except ImportError:
        return False


def is_vscode_cloud_code() -> bool:
    """True if the Cloud Code extension is installed in VS Code."""
    return _is_vscode_extension_installed(GOOGLE_CLOUD_CODE_EXTENSION_NAME)


def is_vscode() -> bool:
    """True if running inside VS Code at all."""
    return os.getenv("VSCODE_PID") is not None


def is_jupyter() -> bool:
    """True if running in a Jupyter environment."""
    return os.getenv("JPY_PARENT_PID") is not None


def is_jupyter_bigquery_plugin_installed() -> bool:
    """True if the BigQuery JupyterLab plugin is installed."""
    return _is_package_installed(BIGQUERY_JUPYTER_PLUGIN_NAME)


def is_jupyter_dataproc_plugin_installed() -> bool:
    """True if the Dataproc Spark Connect Jupyter plugin is installed."""
    return _is_package_installed(DATAPROC_JUPYTER_PLUGIN_NAME)


def is_colab_enterprise() -> bool:
    """True if running in Colab Enterprise (Vertex AI)."""
    return os.getenv("VERTEX_PRODUCT") == "COLAB_ENTERPRISE"


def is_colab() -> bool:
    """True if running in Google Colab."""
    return os.getenv("COLAB_RELEASE_TAG") is not None


def is_workbench() -> bool:
    """True if running in AI Workbench (managed Jupyter)."""
    return os.getenv("VERTEX_PRODUCT") == "WORKBENCH_INSTANCE"


def is_intellij() -> bool:
    """True if running inside IntelliJ IDEA."""
    return os.getenv("IDEA_INITIAL_DIRECTORY") is not None


def is_pycharm() -> bool:
    """True if running inside PyCharm."""
    return os.getenv("PYCHARM_HOSTED") is not None


def is_dataproc_jupyter() -> bool:
    """
    True if either the BigQuery or Dataproc JupyterLab plugin is installed—
    indicating a dataproc-jupyter environment.
    """
    return (
        is_jupyter_bigquery_plugin_installed()
        or is_jupyter_dataproc_plugin_installed()
    )


def get_client_environment_label() -> str:
    """
    Map current environment to a standardized client label.

    Priority order:
      1. Colab Enterprise ("colab-enterprise")
      2. Colab ("colab")
      3. Workbench ("workbench-jupyter")
      4. Dataproc Jupyter ("dataproc-jupyter")
      5. VS Code with Cloud Code ("vscode")
      6. VS Code ("vscode")
      7. IntelliJ ("intellij")
      8. PyCharm ("pycharm")
      9. Jupyter ("jupyter")
     10. Unknown ("unknown")
    """
    checks: List[Tuple[Callable[[], bool], str]] = [
        (is_colab_enterprise, "colab-enterprise"),
        (is_colab, "colab"),
        (is_workbench, "workbench-jupyter"),
        (is_dataproc_jupyter, "dataproc-jupyter"),
        (is_vscode_cloud_code, "vscode-cloud"),
        (is_vscode, "vscode"),
        (is_intellij, "intellij"),
        (is_pycharm, "pycharm"),
        (is_jupyter, "jupyter"),
    ]
    for detector, label in checks:
        try:
            if detector():
                return label
        except Exception:
            pass
    return "unknown"
