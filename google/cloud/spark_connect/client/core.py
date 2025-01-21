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
import google
import grpc
from pyspark.sql.connect.client import ChannelBuilder

from . import proxy


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
        return ProxiedChannel(self.host)

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

    def __init__(self, target_host):
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

    def stream_stream(self, *args, **kwargs):
        return self._wrapped.stream_stream(*args, **kwargs)

    def stream_unary(self, *args, **kwargs):
        return self._wrapped.stream_unary(*args, **kwargs)

    def subscribe(self, *args, **kwargs):
        return self._wrapped.subscribe(*args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        return self._wrapped.unary_stream(*args, **kwargs)

    def unary_unary(self, *args, **kwargs):
        return self._wrapped.unary_unary(*args, **kwargs)

    def unsubscribe(self, *args, **kwargs):
        return self._wrapped.unsubscribe(*args, **kwargs)
