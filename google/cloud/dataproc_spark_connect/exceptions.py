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


def _setup_ipython_exception_handler():
    """Setup custom exception handler for IPython environments to ensure minimal traceback display."""
    try:
        from IPython import get_ipython
    except ImportError:
        return

    ipython = get_ipython()
    if ipython is None:
        return

    # Store original method if not already stored
    if hasattr(ipython, "_dataproc_spark_connect_original_showtraceback"):
        return  # Already patched

    ipython._dataproc_spark_connect_original_showtraceback = (
        ipython.showtraceback
    )

    def custom_showtraceback(
        shell,
        exc_tuple=None,
        filename=None,
        tb_offset=None,
        exception_only=False,
        running_compiled_code=False,
    ):
        # Get the current exception info
        _, value, _ = sys.exc_info() if exc_tuple is None else exc_tuple

        # If it's our custom exception, show only the message
        if isinstance(value, DataprocSparkConnectException):
            print(f"Error: {value.message}", file=sys.stderr)
        else:
            # Use original behavior for other exceptions
            shell._dataproc_spark_connect_original_showtraceback(
                exc_tuple,
                filename,
                tb_offset,
                exception_only,
                running_compiled_code,
            )

    # Override the method
    ipython.showtraceback = custom_showtraceback


# Setup the handler once at module import time
_setup_ipython_exception_handler()


class DataprocSparkConnectException(Exception):
    """A custom exception class to only print the error messages.
    This would be used for exceptions where the stack trace
    doesn't provide any additional information.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(message)

    def _render_traceback_(self):
        return [self.message]
