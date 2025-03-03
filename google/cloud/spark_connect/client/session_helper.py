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
import logging
from typing import Optional

from google.cloud.dataproc_v1.types import sessions

from google.cloud.dataproc_v1 import (
  GetSessionRequest,
  Session,
  SessionControllerClient,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_active_s8s_session_response(
    session_name: str, client_options=None
) -> Optional[sessions.Session]:
  get_session_request = GetSessionRequest()
  get_session_request.name = session_name
  try:
    get_session_response = SessionControllerClient(
        client_options=client_options
    ).get_session(get_session_request)
    state = get_session_response.state
  except Exception as e:
    logger.debug(f"{session_name} deleted: {e}")
    return None

  if state is not None and (
      state == Session.State.ACTIVE or state == Session.State.CREATING
  ):
    return get_session_response
  return None

def is_s8s_session_active(
    session_name : str
) -> bool:
  if get_active_s8s_session_response(session_name) is None:
    return False
  return True
