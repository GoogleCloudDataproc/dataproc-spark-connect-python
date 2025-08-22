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
import sys
from typing import Callable, Tuple, List


def is_vscode() -> bool:
    """True if running inside VS Code at all."""
    return os.getenv("VSCODE_PID") is not None


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
    """True if running in AI Workbench (managed Jupyter)."""
    return os.getenv("VERTEX_PRODUCT") == "WORKBENCH_INSTANCE"


def is_jetbrains_ide() -> bool:
    """True if running inside any JetBrains IDE."""
    return "jetbrains" in os.getenv("TERMINAL_EMULATOR", "").lower()


def is_interactive():
    try:
        from IPython import get_ipython

        if get_ipython() is not None:
            return True
    except ImportError:
        pass

    return hasattr(sys, "ps1") or sys.flags.interactive


def is_terminal():
    return sys.stdin.isatty()


def is_interactive_terminal():
    return is_interactive() and is_terminal()


def get_client_environment_label() -> str:
    """
    Map current environment to a standardized client label.

    Priority order:
      1. Colab Enterprise ("colab-enterprise")
      2. Colab ("colab")
      3. Workbench ("workbench-jupyter")
      4. VS Code ("vscode")
      5. JetBrains IDE ("jetbrains")
      6. Jupyter ("jupyter")
      7. Unknown ("unknown")
    """
    checks: List[Tuple[Callable[[], bool], str]] = [
        (is_colab_enterprise, "colab-enterprise"),
        (is_colab, "colab"),
        (is_workbench, "workbench-jupyter"),
        (is_vscode, "vscode"),
        (is_jetbrains_ide, "jetbrains"),
        (is_jupyter, "jupyter"),
    ]
    for detector, label in checks:
        try:
            if detector():
                return label
        except Exception:
            pass
    return "unknown"
