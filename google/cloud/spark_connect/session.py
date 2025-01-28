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
import atexit
import json
import logging
import os
import random
import string
import time
import datetime
from time import sleep
from typing import Any, cast, ClassVar, Dict, Optional

from google.api_core import retry
from google.api_core.future.polling import POLLING_PREDICATE
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import FailedPrecondition, InvalidArgument, NotFound
from google.cloud.dataproc_v1.types import sessions

from google.cloud.spark_connect.client import DataprocChannelBuilder
from google.cloud.dataproc_v1 import (
    CreateSessionRequest,
    GetSessionRequest,
    Session,
    SessionControllerClient,
    SessionTemplate,
    TerminateSessionRequest,
)
from google.protobuf import text_format
from google.protobuf.text_format import ParseError
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.utils import to_str


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GoogleSparkSession(SparkSession):
    """The entry point to programming Spark with the Dataset and DataFrame API.

    A DataprocRemoteSparkSession can be used to create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    Examples
    --------

    Create a Spark session with Google Spark Connect.

    >>> spark = (
    ...     GoogleSparkSession.builder
    ...         .appName("Word Count")
    ...         .dataprocConfig(Session())
    ...         .getOrCreate()
    ... ) # doctest: +SKIP
    """

    _active_s8s_session_uuid: ClassVar[Optional[str]] = None
    _project_id = None
    _region = None
    _client_options = None
    _active_s8s_session_id: ClassVar[Optional[str]] = None

    class Builder(SparkSession.Builder):

        _dataproc_runtime_spark_version = {"3.0": "3.5.1", "2.2": "3.5.0"}

        _session_static_configs = [
            "spark.executor.cores",
            "spark.executor.memoryOverhead",
            "spark.executor.memory",
            "spark.driver.memory",
            "spark.driver.cores",
            "spark.eventLog.dir",
            "spark.history.fs.logDirectory",
        ]

        def __init__(self):
            self._options: Dict[str, Any] = {}
            self._channel_builder: Optional[DataprocChannelBuilder] = None
            self._dataproc_config: Optional[Session] = None
            self._project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
            self._region = os.environ.get("GOOGLE_CLOUD_REGION")
            self._client_options = ClientOptions(
                api_endpoint=os.environ.get(
                    "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                    f"{self._region}-dataproc.googleapis.com",
                )
            )

        def __apply_options(self, session: "SparkSession") -> None:
            with self._lock:
                self._options = {
                    key: value
                    for key, value in self._options.items()
                    if key not in self._session_static_configs
                }
                self._apply_options(session)

        def projectId(self, project_id):
            self._project_id = project_id
            return self

        def region(self, region):
            self._region = region
            self._client_options.api_endpoint = os.environ.get(
                "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                f"{self._region}-dataproc.googleapis.com",
            )
            return self

        def dataprocConfig(self, dataproc_config: Session):
            with self._lock:
                self._dataproc_config = dataproc_config
                for k, v in dataproc_config.runtime_config.properties.items():
                    self._options[cast(str, k)] = to_str(v)
                return self

        def remote(self, url: Optional[str] = None) -> "SparkSession.Builder":
            if url:
                raise NotImplemented(
                    "GoogleSparkSession does not support connecting to an existing remote server"
                )
            else:
                return self

        def create(self) -> "SparkSession":
            raise NotImplemented(
                "GoogleSparkSession allows session creation only through getOrCreate"
            )

        def __create_spark_connect_session_from_s8s(
            self, session_response
        ) -> "SparkSession":
            GoogleSparkSession._active_s8s_session_uuid = session_response.uuid
            GoogleSparkSession._project_id = self._project_id
            GoogleSparkSession._region = self._region
            GoogleSparkSession._client_options = self._client_options
            spark_connect_url = session_response.runtime_info.endpoints.get(
                "Spark Connect Server"
            )
            spark_connect_url = spark_connect_url.replace("https", "sc")
            url = f"{spark_connect_url.replace('.com/', '.com:443/')};session_id={session_response.uuid};use_ssl=true"
            logger.debug(f"Spark Connect URL: {url}")
            self._channel_builder = DataprocChannelBuilder(url)

            assert self._channel_builder is not None
            session = GoogleSparkSession(connection=self._channel_builder)

            GoogleSparkSession._set_default_and_active_session(session)
            self.__apply_options(session)
            return session

        def __create(self) -> "SparkSession":
            with self._lock:

                if self._options.get("spark.remote", False):
                    raise NotImplemented(
                        "GoogleSparkSession does not support connecting to an existing remote server"
                    )

                from google.cloud.dataproc_v1 import SessionControllerClient

                dataproc_config: Session = self._get_dataproc_config()
                session_template: SessionTemplate = self._get_session_template()

                self._get_and_validate_version(
                    dataproc_config, session_template
                )

                spark_connect_session = self._get_spark_connect_session(
                    dataproc_config, session_template
                )

                if not spark_connect_session:
                    dataproc_config.spark_connect_session = {}
                os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
                session_request = CreateSessionRequest()
                session_id = self.generate_dataproc_session_id()

                session_request.session_id = session_id
                dataproc_config.name = f"projects/{self._project_id}/locations/{self._region}/sessions/{session_id}"
                logger.debug(
                    f"Configurations used to create serverless session:\n {dataproc_config}"
                )
                session_request.session = dataproc_config
                session_request.parent = (
                    f"projects/{self._project_id}/locations/{self._region}"
                )

                logger.debug("Creating serverless session")
                GoogleSparkSession._active_s8s_session_id = session_id
                s8s_creation_start_time = time.time()
                try:
                    session_polling = retry.Retry(
                        predicate=POLLING_PREDICATE,
                        initial=5.0,  # seconds
                        maximum=5.0,  # seconds
                        multiplier=1.0,
                        timeout=600,  # seconds
                    )
                    logger.info(
                        "Creating Spark session. It may take few minutes."
                    )
                    if (
                        "GOOGLE_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT"
                        in os.environ
                        and os.getenv(
                            "GOOGLE_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT"
                        ).lower()
                        == "true"
                    ):
                        atexit.register(
                            lambda: terminate_s8s_session(
                                self._project_id,
                                self._region,
                                session_id,
                                self._client_options,
                            )
                        )
                    operation = SessionControllerClient(
                        client_options=self._client_options
                    ).create_session(session_request)
                    print(
                        f"Interactive Session Detail View:  https://console.cloud.google.com/dataproc/interactive/{self._region}/{session_id}"
                    )
                    session_response: Session = operation.result(
                        polling=session_polling
                    )
                    if (
                        "DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH"
                        in os.environ
                    ):
                        file_path = os.environ[
                            "DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH"
                        ]
                        try:
                            session_data = {
                                "session_name": session_response.name,
                                "session_uuid": session_response.uuid,
                            }
                            os.makedirs(
                                os.path.dirname(file_path), exist_ok=True
                            )
                            with open(file_path, "w") as json_file:
                                json.dump(session_data, json_file, indent=4)
                        except Exception as e:
                            logger.error(
                                f"Exception while writing active session to file {file_path} , {e}"
                            )
                except InvalidArgument as e:
                    GoogleSparkSession._active_s8s_session_id = None
                    raise RuntimeError(
                        f"Error while creating serverless session: {e}"
                    ) from None
                except Exception as e:
                    GoogleSparkSession._active_s8s_session_id = None
                    raise RuntimeError(
                        f"Error while creating serverless session https://console.cloud.google.com/dataproc/interactive/{self._region}/{session_id} : {e}"
                    ) from None

                logger.debug(
                    f"Serverless session created: {session_id}, creation time taken: {int(time.time() - s8s_creation_start_time)} seconds"
                )
                return self.__create_spark_connect_session_from_s8s(
                    session_response
                )

        def _is_s8s_session_active(
            self, s8s_session_id: str
        ) -> Optional[sessions.Session]:
            session_name = f"projects/{self._project_id}/locations/{self._region}/sessions/{s8s_session_id}"
            get_session_request = GetSessionRequest()
            get_session_request.name = session_name
            state = None
            try:
                get_session_response = SessionControllerClient(
                    client_options=self._client_options
                ).get_session(get_session_request)
                state = get_session_response.state
            except Exception as e:
                logger.debug(f"{s8s_session_id} deleted: {e}")
                return None

            if state is not None and (
                state == Session.State.ACTIVE or state == Session.State.CREATING
            ):
                return get_session_response
            return None

        def _get_exiting_active_session(self) -> Optional["SparkSession"]:
            s8s_session_id = GoogleSparkSession._active_s8s_session_id
            session_response = self._is_s8s_session_active(s8s_session_id)

            session = GoogleSparkSession.getActiveSession()
            if session is None:
                session = GoogleSparkSession._default_session

            if session_response is not None:
                print(
                    f"Using existing session: https://console.cloud.google.com/dataproc/interactive/{self._region}/{s8s_session_id}, configuration changes may not be applied."
                )
                if session is None:
                    session = self.__create_spark_connect_session_from_s8s(
                        session_response
                    )
                return session
            else:
                logger.info(
                    f"Session: {s8s_session_id} not active, stopping previous spark session and creating new"
                )
                if session is not None:
                    session.stop()

                return None

        def getOrCreate(self) -> "SparkSession":
            with GoogleSparkSession._lock:
                session = self._get_exiting_active_session()
                if session is None:
                    session = self.__create()
                if session:
                    self.__apply_options(session)
                return session

        def _get_dataproc_config(self):
            dataproc_config = Session()
            if self._dataproc_config:
                dataproc_config = self._dataproc_config
                for k, v in self._options.items():
                    dataproc_config.runtime_config.properties[k] = v
            elif "DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG" in os.environ:
                filepath = os.environ[
                    "DATAPROC_SPARK_CONNECT_SESSION_DEFAULT_CONFIG"
                ]
                try:
                    with open(filepath, "r") as f:
                        dataproc_config = Session.wrap(
                            text_format.Parse(
                                f.read(), Session.pb(dataproc_config)
                            )
                        )
                except FileNotFoundError:
                    raise FileNotFoundError(f"File '{filepath}' not found")
                except ParseError as e:
                    raise ParseError(f"Error parsing file '{filepath}': {e}")
            if "COLAB_NOTEBOOK_RUNTIME_ID" in os.environ:
                dataproc_config.labels["colab-notebook-runtime-id"] = (
                    os.environ["COLAB_NOTEBOOK_RUNTIME_ID"]
                )
            if "COLAB_NOTEBOOK_KERNEL_ID" in os.environ:
                dataproc_config.labels["colab-notebook-kernel-id"] = os.environ[
                    "COLAB_NOTEBOOK_KERNEL_ID"
                ]
            return dataproc_config

        def _get_session_template(self):
            from google.cloud.dataproc_v1 import (
                GetSessionTemplateRequest,
                SessionTemplateControllerClient,
            )

            session_template = None
            if self._dataproc_config and self._dataproc_config.session_template:
                session_template = self._dataproc_config.session_template
                get_session_template_request = GetSessionTemplateRequest()
                get_session_template_request.name = session_template
                client = SessionTemplateControllerClient(
                    client_options=self._client_options
                )
                try:
                    session_template = client.get_session_template(
                        get_session_template_request
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to get session template {session_template}: {e}"
                    )
                    raise
            return session_template

        def _get_and_validate_version(self, dataproc_config, session_template):
            trimmed_version = lambda v: ".".join(v.split(".")[:2])
            version = None
            if (
                dataproc_config
                and dataproc_config.runtime_config
                and dataproc_config.runtime_config.version
            ):
                version = dataproc_config.runtime_config.version
            elif (
                session_template
                and session_template.runtime_config
                and session_template.runtime_config.version
            ):
                version = session_template.runtime_config.version

            if not version:
                version = "3.0"
                dataproc_config.runtime_config.version = version
            elif (
                trimmed_version(version)
                not in self._dataproc_runtime_spark_version
            ):
                raise ValueError(
                    f"runtime_config.version {version} is not supported. "
                    f"Supported versions: {self._dataproc_runtime_spark_version.keys()}"
                )

            server_version = self._dataproc_runtime_spark_version[
                trimmed_version(version)
            ]
            import importlib.metadata

            dataproc_connect_version = importlib.metadata.version(
                "dataproc-spark-connect"
            )
            client_version = importlib.metadata.version("pyspark")
            version_message = f"Spark Connect: {dataproc_connect_version} (PySpark: {client_version}) Session Runtime: {version} (Spark: {server_version})"
            logger.info(version_message)
            if trimmed_version(client_version) != trimmed_version(
                server_version
            ):
                logger.warning(
                    f"client and server on different versions: {version_message}"
                )
            return version

        def _get_spark_connect_session(self, dataproc_config, session_template):
            spark_connect_session = None
            if dataproc_config and dataproc_config.spark_connect_session:
                spark_connect_session = dataproc_config.spark_connect_session
            elif session_template and session_template.spark_connect_session:
                spark_connect_session = session_template.spark_connect_session
            return spark_connect_session

        def generate_dataproc_session_id(self):
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            suffix_length = 6
            random_suffix = "".join(
                random.choices(
                    string.ascii_lowercase + string.digits, k=suffix_length
                )
            )
            return f"sc-{timestamp}-{random_suffix}"

    def _repr_html_(self) -> str:
        if not self._active_s8s_session_id:
            return """
            <div>No Active Dataproc Spark Session</div>
            """

        s8s_session = f"https://console.cloud.google.com/dataproc/interactive/{self._region}/{self._active_s8s_session_id}"
        ui = f"{s8s_session}/sparkApplications/applications"
        version = ""
        return f"""
        <div>
            <p><b>Spark Connect</b></p>

            <p><a href="{s8s_session}">Serverless Session</a></p>
            <p><a href="{ui}">Spark UI</a></p>
        </div>
        """

    def _remove_stoped_session_from_file(self):
        if "DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH" in os.environ:
            file_path = os.environ[
                "DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH"
            ]
            try:
                with open(file_path, "w"):
                    pass
            except Exception as e:
                logger.error(
                    f"Exception while removing active session in file {file_path} , {e}"
                )

    def stop(self) -> None:
        with GoogleSparkSession._lock:
            if GoogleSparkSession._active_s8s_session_id is not None:
                terminate_s8s_session(
                    GoogleSparkSession._project_id,
                    GoogleSparkSession._region,
                    GoogleSparkSession._active_s8s_session_id,
                    self._client_options,
                )

                self._remove_stoped_session_from_file()
                GoogleSparkSession._active_s8s_session_uuid = None
                GoogleSparkSession._active_s8s_session_id = None
                GoogleSparkSession._project_id = None
                GoogleSparkSession._region = None
                GoogleSparkSession._client_options = None

            self.client.close()
            if self is GoogleSparkSession._default_session:
                GoogleSparkSession._default_session = None
            if self is getattr(
                GoogleSparkSession._active_session, "session", None
            ):
                GoogleSparkSession._active_session.session = None


def terminate_s8s_session(
    project_id, region, active_s8s_session_id, client_options=None
):
    from google.cloud.dataproc_v1 import SessionControllerClient

    logger.debug(f"Terminating serverless session: {active_s8s_session_id}")
    terminate_session_request = TerminateSessionRequest()
    session_name = f"projects/{project_id}/locations/{region}/sessions/{active_s8s_session_id}"
    terminate_session_request.name = session_name
    state = None
    try:
        session_client = SessionControllerClient(client_options=client_options)
        session_client.terminate_session(terminate_session_request)
        get_session_request = GetSessionRequest()
        get_session_request.name = session_name
        state = Session.State.ACTIVE
        while (
            state != Session.State.TERMINATING
            and state != Session.State.TERMINATED
            and state != Session.State.FAILED
        ):
            session = session_client.get_session(get_session_request)
            state = session.state
            sleep(1)
    except NotFound:
        logger.debug(f"Session {active_s8s_session_id} already deleted")
    except FailedPrecondition:
        logger.debug(
            f"Session {active_s8s_session_id} already terminated manually or terminated automatically through session ttl limits"
        )
    if state is not None and state == Session.State.FAILED:
        raise RuntimeError("Serverless session termination failed")
