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

import logging
import uuid
from typing import Optional

import google
import grpc
import pyspark.sql.connect.proto as pb2
from pyspark.sql.connect.client import ChannelBuilder, SparkConnectClient

from . import proxy

logger = logging.getLogger(__name__)


class DataprocChannelBuilder(ChannelBuilder):
    """
    This is a helper class that is used to create a GRPC channel based on the given
    connection string per the documentation of Spark Connect.

    This implementation of ChannelBuilder uses `secure_authorized_channel` from the
    `google.auth.transport.grpc` package for authenticating secure channel.

    Examples
    --------
    >>> cb =  ChannelBuilder("sc://localhost")
    ... cb.endpoint

    >>> cb = ChannelBuilder("sc://localhost/;use_ssl=true;token=aaa")
    ... cb.secure
    True
    """

    def __init__(self, url, is_active_callback=None):
        self._is_active_callback = is_active_callback
        super().__init__(url)

    def toChannel(self) -> grpc.Channel:
        """
        Applies the parameters of the connection string and creates a new
        GRPC channel according to the configuration. Passes optional channel options to
        construct the channel.

        Returns
        -------
        GRPC Channel instance.
        """
        # TODO: Replace with a direct channel once all compatibility issues with
        # grpc have been resolved.
        return self._proxied_channel()

    def _proxied_channel(self) -> grpc.Channel:
        return ProxiedChannel(self.host, self._is_active_callback)

    def _direct_channel(self) -> grpc.Channel:
        destination = f"{self.host}:{self.port}"

        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        # Get an HTTP request function to refresh credentials.
        request = google.auth.transport.requests.Request()
        # Create a channel.

        return google.auth.transport.grpc.secure_authorized_channel(
            credentials,
            request,
            destination,
            None,
            None,
            options=self._channel_options,
        )


class ProxiedChannel(grpc.Channel):

    def __init__(self, target_host, is_active_callback):
        self._is_active_callback = is_active_callback
        self._proxy = proxy.DataprocSessionProxy(0, target_host)
        self._proxy.start()
        self._proxied_connect_url = f"sc://localhost:{self._proxy.port}"
        self._wrapped = ChannelBuilder(self._proxied_connect_url).toChannel()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        ret = self._wrapped.__exit__(*args)
        self._proxy.stop()
        return ret

    def close(self):
        ret = self._wrapped.close()
        self._proxy.stop()
        return ret

    def _wrap_method(self, wrapped_method):
        if self._is_active_callback is None:
            return wrapped_method

        def checked_method(*margs, **mkwargs):
            if (
                self._is_active_callback is not None
                and not self._is_active_callback()
            ):
                logger.warning(f"Session is no longer active")
                raise RuntimeError(
                    "Session not active. Please create a new session"
                )
            return wrapped_method(*margs, **mkwargs)

        return checked_method

    def stream_stream(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.stream_stream(*args, **kwargs))

    def stream_unary(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.stream_unary(*args, **kwargs))

    def subscribe(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.subscribe(*args, **kwargs))

    def unary_stream(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.unary_stream(*args, **kwargs))

    def unary_unary(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.unary_unary(*args, **kwargs))

    def unsubscribe(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.unsubscribe(*args, **kwargs))


def _generate_dataproc_operation_id() -> str:
    """
    If an operation_id is not supplied in the ExecutePlanRequest, one is
    generated and supplied by the dataproc client.

    :return: UUID string of format '00112233-4455-6677-8899-aabbccddeeff'
    """
    return str(uuid.uuid4())


class DataprocSparkConnectClient(SparkConnectClient):
    """
    The remote spark session in Dataproc that communicates with the server.
    Replaces the default :py:class:`SparkConnectClient` as the client for
    :py:class:`DataprocSparkSession`.
    """

    # keep track of the active / most recent ExecutePlanRequest's operation_id
    _last_operation_id: Optional[str] = None

    def _execute_plan_request_with_metadata(self) -> pb2.ExecutePlanRequest:
        req = super()._execute_plan_request_with_metadata()
        if not req.operation_id:
            req.operation_id = _generate_dataproc_operation_id()
            logger.debug(
                f"No operation_id found. Setting operation_id: {req.operation_id}"
            )
        self._last_operation_id = req.operation_id
        return req

    @property
    def latest_operation_id(self) -> Optional[str]:
        """
        Operation ID is not an inherent property of the client itself, rather it
        is the operation_id of the last request handled by the client.

        :return: operation_id of the current / most recent ExecutePlanRequest
        """
        return self._last_operation_id
