# Copyright 2026 Google LLC
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

"""CloudEvent parsing and payload validation utilities for Gen 2 Cloud Run services."""

import base64
import json
import logging
from typing import Any, Optional

import jsonschema


def parse_pubsub_cloudevent(
    cloud_event: Any,
    schema: Optional[dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[dict[str, Any]]:
  """Extracts, decodes, and validates a JSON payload from a Pub/Sub CloudEvent.

  Args:
      cloud_event: The CloudEvent object passed to the entrypoint.
      schema: Optional JSON schema dictionary to validate the payload against.
      logger: Optional logger for emitting debug/error messages.

  Returns:
      A dictionary containing the parsed JSON message payload, or None if
      decoding or schema validation failed.
  """
  log = logger or logging.getLogger(__name__)

  try:
    if not hasattr(cloud_event, 'data') or not cloud_event.data:
      log.error('CloudEvent contains no data attribute: %s', cloud_event)
      return None

    event_data = cloud_event.data
    if isinstance(event_data, dict) and 'message' in event_data:
      pubsub_message = event_data['message']
    elif isinstance(event_data, dict):
      pubsub_message = event_data
    else:
      log.error('Unexpected CloudEvent data format: %s', type(event_data))
      return None

    raw_data = pubsub_message.get('data', '')
    if not raw_data:
      log.error('Pub/Sub message contains empty data field: %s', pubsub_message)
      return None

    if isinstance(raw_data, bytes):
      decoded_str = base64.b64decode(raw_data).decode('utf-8')
    elif isinstance(raw_data, str):
      decoded_str = base64.b64decode(raw_data.encode('utf-8')).decode('utf-8')
    else:
      log.error('Invalid raw data type: %s', type(raw_data))
      return None

    payload = json.loads(decoded_str)
    if not isinstance(payload, dict):
      log.error('Parsed payload is not a JSON object: %s', type(payload))
      return None

    if schema:
      jsonschema.validate(instance=payload, schema=schema)

    return payload

  except jsonschema.exceptions.ValidationError as e:
    log.error(
        'Pub/Sub message failed schema validation: %s. Message payload: %s',
        e.message,
        locals().get('decoded_str', '<undecoded>'),
        exc_info=True,
    )
    return None

  except Exception as e:  # pylint: disable=broad-except
    log.error(
        'Failed to decode CloudEvent Pub/Sub payload: %s', e, exc_info=True
    )
    return None
