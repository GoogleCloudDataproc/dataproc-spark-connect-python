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
import datetime
import json
import logging
import os
import random
import re
import string
import threading
import time
from typing import Any, cast, ClassVar, Dict, Optional, Union
import uuid
import tqdm

from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import (
    Aborted,
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    PermissionDenied,
)
from google.api_core.future.polling import POLLING_PREDICATE
from google.cloud.dataproc_spark_connect.client import DataprocChannelBuilder
from google.cloud.dataproc_spark_connect.exceptions import DataprocSparkConnectException
from google.cloud.dataproc_spark_connect.pypi_artifacts import PyPiArtifacts
from google.cloud.dataproc_v1 import (
    AuthenticationConfig,
    CreateSessionRequest,
    GetSessionRequest,
    Session,
    SessionControllerClient,
    TerminateSessionRequest,
)
from google.cloud.dataproc_v1.types import sessions
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.utils import to_str

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _is_valid_label_value(value: str) -> bool:
    """
    Validates if a string complies with Google Cloud label value format.
    Only lowercase letters, numbers, and dashes are allowed.
    The value must start with lowercase letter or number and end with a lowercase letter or number.
    Maximum length is 63 characters.
    """
    if not value:
        return False

    # Check maximum length (63 characters for GCP label values)
    if len(value) > 63:
        return False

    # Check if the value matches the pattern: starts and ends with alphanumeric,
    # contains only lowercase letters, numbers, and dashes
    pattern = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"
    return bool(re.match(pattern, value))


class DataprocSparkSession(SparkSession):
    """The entry point to programming Spark with the Dataset and DataFrame API.

    A DataprocRemoteSparkSession can be used to create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    Examples
    --------

    Create a Spark session with Dataproc Spark Connect.

    >>> spark = (
    ...     DataprocSparkSession.builder
    ...         .appName("Word Count")
    ...         .dataprocSessionConfig(Session())
    ...         .getOrCreate()
    ... ) # doctest: +SKIP
    """

    _DEFAULT_RUNTIME_VERSION = "2.3"

    _active_s8s_session_uuid: ClassVar[Optional[str]] = None
    _project_id = None
    _region = None
    _client_options = None
    _active_s8s_session_id: ClassVar[Optional[str]] = None

    class Builder(SparkSession.Builder):

        def __init__(self):
            self._options: Dict[str, Any] = {}
            self._channel_builder: Optional[DataprocChannelBuilder] = None
            self._dataproc_config: Optional[Session] = None
            self._project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
            self._region = os.getenv("GOOGLE_CLOUD_REGION")
            self._client_options = ClientOptions(
                api_endpoint=os.getenv(
                    "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                    f"{self._region}-dataproc.googleapis.com",
                )
            )

        def projectId(self, project_id):
            self._project_id = project_id
            return self

        def location(self, location):
            self._region = location
            self._client_options.api_endpoint = os.getenv(
                "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                f"{self._region}-dataproc.googleapis.com",
            )
            return self

        def dataprocSessionConfig(self, dataproc_config: Session):
            with self._lock:
                self._dataproc_config = dataproc_config
                for k, v in dataproc_config.runtime_config.properties.items():
                    self._options[cast(str, k)] = to_str(v)
                return self

        def remote(self, url: Optional[str] = None) -> "SparkSession.Builder":
            if url:
                raise NotImplemented(
                    "DataprocSparkSession does not support connecting to an existing remote server"
                )
            else:
                return self

        def create(self) -> "DataprocSparkSession":
            raise NotImplemented(
                "DataprocSparkSession allows session creation only through getOrCreate"
            )

        def __create_spark_connect_session_from_s8s(
            self, session_response, session_name
        ) -> "DataprocSparkSession":
            DataprocSparkSession._active_s8s_session_uuid = (
                session_response.uuid
            )
            DataprocSparkSession._project_id = self._project_id
            DataprocSparkSession._region = self._region
            DataprocSparkSession._client_options = self._client_options
            spark_connect_url = session_response.runtime_info.endpoints.get(
                "Spark Connect Server"
            )
            url = f"{spark_connect_url}/;session_id={session_response.uuid};use_ssl=true"
            logger.debug(f"Spark Connect URL: {url}")
            self._channel_builder = DataprocChannelBuilder(
                url,
                is_active_callback=lambda: is_s8s_session_active(
                    session_name, self._client_options
                ),
            )

            assert self._channel_builder is not None
            session = DataprocSparkSession(connection=self._channel_builder)

            DataprocSparkSession._set_default_and_active_session(session)
            return session

        def __create(self) -> "DataprocSparkSession":
            with self._lock:

                if self._options.get("spark.remote", False):
                    raise NotImplemented(
                        "DataprocSparkSession does not support connecting to an existing Spark Connect remote server"
                    )

                from google.cloud.dataproc_v1 import SessionControllerClient

                dataproc_config: Session = self._get_dataproc_config()

                session_id = self.generate_dataproc_session_id()
                dataproc_config.name = f"projects/{self._project_id}/locations/{self._region}/sessions/{session_id}"
                logger.debug(
                    f"Dataproc Session configuration:\n{dataproc_config}"
                )

                session_request = CreateSessionRequest()
                session_request.session_id = session_id
                session_request.session = dataproc_config
                session_request.parent = (
                    f"projects/{self._project_id}/locations/{self._region}"
                )

                logger.debug("Creating Dataproc Session")
                DataprocSparkSession._active_s8s_session_id = session_id
                s8s_creation_start_time = time.time()

                stop_create_session_pbar_event = threading.Event()

                def create_session_pbar():
                    iterations = 150
                    pbar = tqdm.trange(
                        iterations,
                        bar_format="{bar}",
                        ncols=80,
                    )
                    for i in pbar:
                        if stop_create_session_pbar_event.is_set():
                            break
                        # Last iteration
                        if i >= iterations - 1:
                            # Sleep until session created
                            while not stop_create_session_pbar_event.is_set():
                                time.sleep(1)
                        else:
                            time.sleep(1)

                    pbar.close()
                    # Print new line after the progress bar
                    print()

                create_session_pbar_thread = threading.Thread(
                    target=create_session_pbar
                )

                try:
                    if (
                        os.getenv(
                            "DATAPROC_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT",
                            "false",
                        )
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
                        f"Creating Dataproc Session: https://console.cloud.google.com/dataproc/interactive/{self._region}/{session_id}?project={self._project_id}"
                    )
                    self._display_view_session_details_button(session_id)
                    create_session_pbar_thread.start()
                    session_response: Session = operation.result(
                        polling=retry.Retry(
                            predicate=POLLING_PREDICATE,
                            initial=5.0,  # seconds
                            maximum=5.0,  # seconds
                            multiplier=1.0,
                            timeout=600,  # seconds
                        )
                    )
                    stop_create_session_pbar_event.set()
                    create_session_pbar_thread.join()
                    print("Dataproc Session was successfully created")
                    file_path = (
                        DataprocSparkSession._get_active_session_file_path()
                    )
                    if file_path is not None:
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
                                f"Exception while writing active session to file {file_path}, {e}"
                            )
                except (InvalidArgument, PermissionDenied) as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    DataprocSparkSession._active_s8s_session_id = None
                    raise DataprocSparkConnectException(
                        f"Error while creating Dataproc Session: {e.message}"
                    )
                except Exception as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    DataprocSparkSession._active_s8s_session_id = None
                    raise RuntimeError(
                        f"Error while creating Dataproc Session"
                    ) from e
                finally:
                    stop_create_session_pbar_event.set()

                logger.debug(
                    f"Dataproc Session created: {session_id} in {int(time.time() - s8s_creation_start_time)} seconds"
                )
                return self.__create_spark_connect_session_from_s8s(
                    session_response, dataproc_config.name
                )

        def _get_exiting_active_session(
            self,
        ) -> Optional["DataprocSparkSession"]:
            s8s_session_id = DataprocSparkSession._active_s8s_session_id
            session_name = f"projects/{self._project_id}/locations/{self._region}/sessions/{s8s_session_id}"
            session_response = None
            session = None
            if s8s_session_id is not None:
                session_response = get_active_s8s_session_response(
                    session_name, self._client_options
                )
                session = DataprocSparkSession.getActiveSession()

            if session is None:
                session = DataprocSparkSession._default_session

            if session_response is not None:
                print(
                    f"Using existing Dataproc Session (configuration changes may not be applied): https://console.cloud.google.com/dataproc/interactive/{self._region}/{s8s_session_id}?project={self._project_id}"
                )
                self._display_view_session_details_button(s8s_session_id)
                if session is None:
                    session = self.__create_spark_connect_session_from_s8s(
                        session_response, session_name
                    )
                return session
            else:
                if session is not None:
                    print(
                        f"{s8s_session_id} Dataproc Session is not active, stopping and creating a new one"
                    )
                    session.stop()

                return None

        def getOrCreate(self) -> "DataprocSparkSession":
            with DataprocSparkSession._lock:
                session = self._get_exiting_active_session()
                if session is None:
                    session = self.__create()
                return session

        def _get_dataproc_config(self):
            dataproc_config = Session()
            if self._dataproc_config:
                dataproc_config = self._dataproc_config
                for k, v in self._options.items():
                    dataproc_config.runtime_config.properties[k] = v
            dataproc_config.spark_connect_session = (
                sessions.SparkConnectConfig()
            )
            if not dataproc_config.runtime_config.version:
                dataproc_config.runtime_config.version = (
                    DataprocSparkSession._DEFAULT_RUNTIME_VERSION
                )
            if (
                not dataproc_config.environment_config.execution_config.authentication_config.user_workload_authentication_type
                and "DATAPROC_SPARK_CONNECT_AUTH_TYPE" in os.environ
            ):
                dataproc_config.environment_config.execution_config.authentication_config.user_workload_authentication_type = AuthenticationConfig.AuthenticationType[
                    os.getenv("DATAPROC_SPARK_CONNECT_AUTH_TYPE")
                ]
            if (
                not dataproc_config.environment_config.execution_config.service_account
                and "DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT" in os.environ
            ):
                dataproc_config.environment_config.execution_config.service_account = os.getenv(
                    "DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT"
                )
            if (
                not dataproc_config.environment_config.execution_config.subnetwork_uri
                and "DATAPROC_SPARK_CONNECT_SUBNET" in os.environ
            ):
                dataproc_config.environment_config.execution_config.subnetwork_uri = os.getenv(
                    "DATAPROC_SPARK_CONNECT_SUBNET"
                )
            if (
                not dataproc_config.environment_config.execution_config.ttl
                and "DATAPROC_SPARK_CONNECT_TTL_SECONDS" in os.environ
            ):
                dataproc_config.environment_config.execution_config.ttl = {
                    "seconds": int(
                        os.getenv("DATAPROC_SPARK_CONNECT_TTL_SECONDS")
                    )
                }
            if (
                not dataproc_config.environment_config.execution_config.idle_ttl
                and "DATAPROC_SPARK_CONNECT_IDLE_TTL_SECONDS" in os.environ
            ):
                dataproc_config.environment_config.execution_config.idle_ttl = {
                    "seconds": int(
                        os.getenv("DATAPROC_SPARK_CONNECT_IDLE_TTL_SECONDS")
                    )
                }
            if "COLAB_NOTEBOOK_ID" in os.environ:
                colab_notebook_name = os.environ["COLAB_NOTEBOOK_ID"]
                # Extract the last part of the path, which is the ID
                notebook_id = os.path.basename(colab_notebook_name)
                if _is_valid_label_value(notebook_id):
                    dataproc_config.labels["goog-colab-notebook-id"] = (
                        notebook_id
                    )
                else:
                    logger.warning(
                        f"Warning while processing notebook ID: Notebook ID '{notebook_id}' is not compliant with label value format. "
                        f"Only lowercase letters, numbers, and dashes are allowed. "
                        f"The value must start with lowercase letter or number and end with a lowercase letter or number. "
                        f"Maximum length is 63 characters. "
                        f"Skipping notebook ID label."
                    )
            default_datasource = os.getenv(
                "DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE"
            )
            if (
                default_datasource
                and dataproc_config.runtime_config.version == "2.3"
            ):
                if default_datasource == "bigquery":
                    bq_datasource_properties = {
                        "spark.datasource.bigquery.viewsEnabled": "true",
                        "spark.datasource.bigquery.writeMethod": "direct",
                        "spark.sql.catalog.spark_catalog": "com.google.cloud.spark.bigquery.BigQuerySparkSessionCatalog",
                        "spark.sql.legacy.createHiveTableByDefault": "false",
                        "spark.sql.sources.default": "bigquery",
                    }
                    # Merge default configs with existing properties, user configs take precedence
                    for k, v in bq_datasource_properties.items():
                        if k not in dataproc_config.runtime_config.properties:
                            dataproc_config.runtime_config.properties[k] = v
                else:
                    logger.warning(
                        f"DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value:"
                        f" {default_datasource}. Supported value is 'bigquery'."
                    )
            return dataproc_config

        def _display_view_session_details_button(self, session_id):
            try:
                session_url = f"https://console.cloud.google.com/dataproc/interactive/sessions/{session_id}/locations/{self._region}?project={self._project_id}"
                from google.cloud.aiplatform.utils import _ipython_utils

                _ipython_utils.display_link(
                    "View Session Details", f"{session_url}", "dashboard"
                )
            except ImportError as e:
                logger.debug(f"Import error: {e}")

        @staticmethod
        def generate_dataproc_session_id():
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            suffix_length = 6
            random_suffix = "".join(
                random.choices(
                    string.ascii_lowercase + string.digits, k=suffix_length
                )
            )
            return f"sc-{timestamp}-{random_suffix}"

    def __init__(
        self,
        connection: Union[str, DataprocChannelBuilder],
        user_id: Optional[str] = None,
    ):
        """
        Creates a new DataprocSparkSession for the Spark Connect interface.

        Parameters
        ----------
        connection : str or :class:`DataprocChannelBuilder`
            Connection string that is used to extract the connection parameters
            and configure the GRPC connection. Or instance of ChannelBuilder /
            DataprocChannelBuilder that creates GRPC connection.
        user_id : str, optional
            If not set, will default to the $USER environment. Defining the user
            ID as part of the connection string takes precedence.
        """

        super().__init__(connection, user_id)

        base_method = self.client._execute_plan_request_with_metadata

        def wrapped_method(*args, **kwargs):
            req = base_method(*args, **kwargs)
            if not req.operation_id:
                req.operation_id = str(uuid.uuid4())
                logger.debug(
                    f"No operation_id found. Setting operation_id: {req.operation_id}"
                )
            self._display_operation_link(req.operation_id)
            return req

        self.client._execute_plan_request_with_metadata = wrapped_method

    def _repr_html_(self) -> str:
        if not self._active_s8s_session_id:
            return """
            <div>No Active Dataproc Session</div>
            """

        s8s_session = f"https://console.cloud.google.com/dataproc/interactive/{self._region}/{self._active_s8s_session_id}"
        ui = f"{s8s_session}/sparkApplications/applications"
        return f"""
        <div>
            <p><b>Spark Connect</b></p>

            <p><a href="{s8s_session}?project={self._project_id}">Dataproc Session</a></p>
            <p><a href="{ui}?project={self._project_id}">Spark UI</a></p>
        </div>
        """

    def _display_operation_link(self, operation_id: str):
        assert all(
            [
                operation_id is not None,
                self._region is not None,
                self._active_s8s_session_id is not None,
                self._project_id is not None,
            ]
        )

        url = (
            f"https://console.cloud.google.com/dataproc/interactive/{self._region}/"
            f"{self._active_s8s_session_id}/sparkApplications/application;"
            f"associatedSqlOperationId={operation_id}?project={self._project_id}"
        )

        try:
            from IPython.display import display, HTML
            from IPython.core.interactiveshell import InteractiveShell

            if not InteractiveShell.initialized():
                return
            html_element = f"""
              <div>
                  <p><a href="{url}">Spark UI</a> (Operation: {operation_id})</p>
              </div>
              """
            display(HTML(html_element))
        except ImportError:
            return

    @staticmethod
    def _remove_stopped_session_from_file():
        file_path = DataprocSparkSession._get_active_session_file_path()
        if file_path is not None:
            try:
                with open(file_path, "w"):
                    pass
            except Exception as e:
                logger.error(
                    f"Exception while removing active session in file {file_path}, {e}"
                )

    def addArtifacts(
        self,
        *artifact: str,
        pyfile: bool = False,
        archive: bool = False,
        file: bool = False,
        pypi: bool = False,
    ) -> None:
        """
        Add artifact(s) to the client session. Currently only local files & pypi installations are supported.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        *artifact : tuple of str
            Artifact's URIs to add.
        pyfile : bool
            Whether to add them as Python dependencies such as .py, .egg, .zip or .jar files.
            The pyfiles are directly inserted into the path when executing Python functions
            in executors.
        archive : bool
            Whether to add them as archives such as .zip, .jar, .tar.gz, .tgz, or .tar files.
            The archives are unpacked on the executor side automatically.
        file : bool
            Add a file to be downloaded with this Spark job on every node.
            The ``path`` passed can only be a local file for now.
        pypi : bool
            This option is only available with DataprocSparkSession. e.g. `spark.addArtifacts("spacy==3.8.4", "torch",  pypi=True)`
            Installs PyPi package (with its dependencies) in the active Spark session on the driver and executors.

        Notes
        -----
        This is an API dedicated to Spark Connect client only. With regular Spark Session, it throws
        an exception.
        Regarding pypi: Popular packages are already pre-installed in s8s runtime.
        https://cloud.google.com/dataproc-serverless/docs/concepts/versions/spark-runtime-2.2#python_libraries
        If there are conflicts/package doesn't exist, it throws an exception.
        """
        if sum([pypi, file, pyfile, archive]) > 1:
            raise ValueError(
                "'pyfile', 'archive', 'file' and/or 'pypi' cannot be True together."
            )
        if pypi:
            artifacts = PyPiArtifacts(set(artifact))
            logger.debug("Making addArtifact call to install packages")
            self.addArtifact(
                artifacts.write_packages_config(self._active_s8s_session_uuid),
                file=True,
            )
        else:
            super().addArtifacts(
                *artifact, pyfile=pyfile, archive=archive, file=file
            )

    @staticmethod
    def _get_active_session_file_path():
        return os.getenv("DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH")

    def stop(self) -> None:
        with DataprocSparkSession._lock:
            if DataprocSparkSession._active_s8s_session_id is not None:
                terminate_s8s_session(
                    DataprocSparkSession._project_id,
                    DataprocSparkSession._region,
                    DataprocSparkSession._active_s8s_session_id,
                    self._client_options,
                )

                self._remove_stopped_session_from_file()
                DataprocSparkSession._active_s8s_session_uuid = None
                DataprocSparkSession._active_s8s_session_id = None
                DataprocSparkSession._project_id = None
                DataprocSparkSession._region = None
                DataprocSparkSession._client_options = None

            self.client.close()
            if self is DataprocSparkSession._default_session:
                DataprocSparkSession._default_session = None
            if self is getattr(
                DataprocSparkSession._active_session, "session", None
            ):
                DataprocSparkSession._active_session.session = None


def terminate_s8s_session(
    project_id, region, active_s8s_session_id, client_options=None
):
    from google.cloud.dataproc_v1 import SessionControllerClient

    logger.debug(f"Terminating Dataproc Session: {active_s8s_session_id}")
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
            time.sleep(1)
    except NotFound:
        logger.debug(
            f"{active_s8s_session_id} Dataproc Session already deleted"
        )
    # Client will get 'Aborted' error if session creation is still in progress and
    # 'FailedPrecondition' if another termination is still in progress.
    # Both are retryable, but we catch it and let TTL take care of cleanups.
    except (FailedPrecondition, Aborted):
        logger.debug(
            f"{active_s8s_session_id} Dataproc Session already terminated manually or automatically due to TTL"
        )
    if state is not None and state == Session.State.FAILED:
        raise RuntimeError("Dataproc Session termination failed")


def get_active_s8s_session_response(
    session_name, client_options
) -> Optional[sessions.Session]:
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    try:
        get_session_response = SessionControllerClient(
            client_options=client_options
        ).get_session(get_session_request)
        state = get_session_response.state
    except Exception as e:
        print(f"{session_name} Dataproc Session deleted: {e}")
        return None
    if state is not None and (
        state == Session.State.ACTIVE or state == Session.State.CREATING
    ):
        return get_session_response
    return None


def is_s8s_session_active(session_name, client_options) -> bool:
    if get_active_s8s_session_response(session_name, client_options) is None:
        return False
    return True
