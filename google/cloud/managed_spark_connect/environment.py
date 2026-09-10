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

import functools
import os
import subprocess
import sys
from typing import Callable, Tuple, List


def is_antigravity() -> bool:
    """True if running inside the Antigravity environment."""
    return "antigravity" in os.getenv("__CFBundleIdentifier", "").lower()


def is_vscode() -> bool:
    """True if running inside VS Code at all."""
    return os.getenv("VSCODE_PID") is not None


@functools.lru_cache(maxsize=1)
def _installed_vscode_extensions() -> frozenset:
    try:
        result = subprocess.run(
            ["code", "--list-extensions"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return frozenset()
    return frozenset(
        line.strip().lower()
        for line in result.stdout.splitlines()
        if line.strip()
    )


def _vscode_extensions_dirs() -> Tuple[str, ...]:
    home = os.path.expanduser("~")
    return (
        os.path.join(home, ".vscode", "extensions"),
        os.path.join(home, ".vscode-server", "extensions"),
        os.path.join(home, ".vscode-insiders", "extensions"),
        os.path.join(home, ".vscode-server-insiders", "extensions"),
    )


@functools.lru_cache(maxsize=None)
def _is_extension_dir_present(extension_id: str) -> bool:
    prefix = f"{extension_id}-"
    for extensions_dir in _vscode_extensions_dirs():
        try:
            entries = os.listdir(extensions_dir)
        except OSError:
            continue
        if any(name.lower().startswith(prefix) for name in entries):
            return True
    return False


def _is_remote_vscode_server() -> bool:
    """True if this process is running as a vscode-server backend
    (Remote-SSH, Tunnels, Dev Containers), as opposed to local desktop
    VS Code.
    """
    return os.path.isdir(
        os.path.join(os.path.expanduser("~"), ".vscode-server")
    )


def is_vscode_extension_installed(extension_id: str) -> bool:
    """True if the given VS Code extension id is installed.

    Checks via the `code` CLI first, falling back to scanning the
    on-disk extensions directories directly, since `code` may not be on
    PATH even when VS Code and the extension are installed (e.g. macOS
    without "Shell Command: Install 'code' command in PATH" run). On a
    vscode-server (Remote-SSH) backend, `code` runs through a
    client-forwarding shim with unreliable `--list-extensions` behavior,
    so only the disk scan is used there. Fails closed (returns False)
    if nothing finds it.
    """
    extension_id = extension_id.lower()
    if _is_remote_vscode_server():
        return _is_extension_dir_present(extension_id)
    return extension_id in _installed_vscode_extensions() or (
        _is_extension_dir_present(extension_id)
    )


def is_jupyter() -> bool:
    """True if running in a Jupyter environment."""
    return os.getenv("JPY_PARENT_PID") is not None


def is_colab_enterprise() -> bool:
    """True if running in Colab Enterprise (Vertex AI)."""
    return os.getenv("VERTEX_PRODUCT") == "COLAB_ENTERPRISE"


def is_colab() -> bool:
    """True if running in Google Colab."""
    return os.getenv("COLAB_RELEASE_TAG") is not None


def is_workbench() -> bool:
    """True if running in Vertex Workbench Instance (managed Jupyter)."""
    return os.getenv("VERTEX_PRODUCT") == "WORKBENCH_INSTANCE"


def is_kaggle() -> bool:
    """True if running in Kaggle Notebooks."""
    return os.getenv("KAGGLE_KERNEL_RUN_TYPE") is not None


def is_databricks() -> bool:
    """True if running in Databricks."""
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


def is_sagemaker() -> bool:
    """True if running in AWS SageMaker."""
    return os.getenv("SAGEMAKER_INTERNAL_IMAGE_URI") is not None


def is_deepnote() -> bool:
    """True if running in Deepnote."""
    return os.getenv("DEEPNOTE_PROJECT_ID") is not None


def is_datalore() -> bool:
    """True if running in JetBrains Datalore."""
    return os.getenv("DATALORE_USER") is not None


def is_spyder() -> bool:
    """True if running inside Spyder IDE."""
    return any(k.startswith("SPYDER") for k in os.environ)


def is_cloud_shell() -> bool:
    """True if running in Google Cloud Shell."""
    return os.getenv("CLOUD_SHELL") is not None


def is_codespaces() -> bool:
    """True if running in GitHub Codespaces."""
    return os.getenv("CODESPACES") is not None


def is_jetbrains_ide() -> bool:
    """True if running inside JetBrains IDE."""
    return (
        "jetbrains" in os.getenv("TERMINAL_EMULATOR", "").lower()
        or "PYCHARM_HOSTED" in os.environ
    )


def is_hex() -> bool:
    """True if running in Hex."""
    return os.getenv("HEX_PROJECT_ID") is not None


def is_polynote() -> bool:
    """True if running in Polynote."""
    return os.getenv("POLYNOTE_VERSION") is not None


def is_eclipse() -> bool:
    """True if running inside Eclipse IDE."""
    return "ECLIPSE_HOME" in os.environ or any(
        k.startswith("ECLIPSE") for k in os.environ
    )


def is_interactive() -> bool:
    try:
        from IPython import get_ipython

        if get_ipython() is not None:
            return True
    except ImportError:
        pass

    return hasattr(sys, "ps1") or bool(sys.flags.interactive)


def is_terminal() -> bool:
    return sys.stdin.isatty()


def is_interactive_terminal() -> bool:
    return is_interactive() and is_terminal()


def is_dataproc_batch() -> bool:
    return os.getenv("DATAPROC_WORKLOAD_TYPE") == "batch"


def get_client_environment_label() -> str:
    """
    Map current environment to a standardized client label.

    Priority order:
      1. Colab Enterprise ("colab-enterprise")
      2. Colab ("colab")
      3. Vertex Workbench Instance ("workbench-jupyter")
      4. Kaggle ("kaggle")
      5. AWS SageMaker ("sagemaker")
      6. Databricks ("databricks")
      7. Deepnote ("deepnote")
      8. JetBrains Datalore ("datalore")
      9. GitHub Codespaces ("codespaces")
      10. Google Cloud Shell ("cloud-shell")
      11. Hex ("hex")
      12. Polynote ("polynote")
      13. Antigravity ("antigravity")
      14. VS Code ("vscode")
      15. JetBrains IDE ("jetbrains")
      16. Spyder ("spyder")
      17. Eclipse ("eclipse")
      18. Jupyter ("jupyter")
      19. Unknown ("unknown")
    """
    checks: List[Tuple[Callable[[], bool], str]] = [
        (is_colab_enterprise, "colab-enterprise"),
        (is_colab, "colab"),
        (is_workbench, "workbench-jupyter"),
        (is_kaggle, "kaggle"),
        (is_sagemaker, "sagemaker"),
        (is_databricks, "databricks"),
        (is_deepnote, "deepnote"),
        (is_datalore, "datalore"),
        (is_codespaces, "codespaces"),
        (is_cloud_shell, "cloud-shell"),
        (is_hex, "hex"),
        (is_polynote, "polynote"),
        (is_antigravity, "antigravity"),
        (is_vscode, "vscode"),
        (is_jetbrains_ide, "jetbrains"),
        (is_spyder, "spyder"),
        (is_eclipse, "eclipse"),
        (is_jupyter, "jupyter"),
    ]
    for detector, label in checks:
        try:
            if detector():
                return label
        except Exception:
            pass
    return "unknown"
