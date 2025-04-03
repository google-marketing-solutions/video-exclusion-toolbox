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

"""Dispatch Youtube Video IDs for processing, one by one.

Retrieve Youtube Video IDs from a BQ table's partition, filters the ones that
need to be processed and dispatches them to a Pub/Sub topic.
"""

from concurrent import futures
import functools
import json
import logging
import math
import os
import sys

import flask
import functions_framework
import google.auth
from google.cloud import bigquery
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from googleapiclient import discovery
import jsonschema


# Fetched from variables available by default.
CLOUD_FUNCTION_NAME = os.environ.get(
    'K_SERVICE', 'failed_to_get_cloud_function_name'
)

# Set up logging either for local testing or for Cloud Run
LOG_LEVEL = logging.INFO
if os.getenv('IS_LOCAL_TEST', 'False') == 'True':
  logging.basicConfig(level=LOG_LEVEL, stream=sys.stdout)
else:
  logging_client = cloud_logging.Client()
  logging_client.setup_logging(log_level=LOG_LEVEL)

# Suppress the default logging from submodules.
logging.getLogger('google_genai').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

logger = logging.getLogger(CLOUD_FUNCTION_NAME)

GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
PUBSUB_TARGET_TOPIC = os.environ.get('VET_THUMBNAIL_AGE_EVALUATION_TOPIC')
BQ_SOURCE_DATASET = os.environ.get('VET_BIGQUERY_SOURCE_DATASET')
BQ_TARGET_DATASET = os.environ.get('VET_BIGQUERY_TARGET_DATASET')
BQ_SOURCE_TABLE = os.environ.get('VET_BIGQUERY_SOURCE_TABLE')
BQ_TARGET_TABLE = os.environ.get('VET_BIGQUERY_TARGET_TABLE')

# The access scopes used in this function
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The schema of the JSON in the event payload.
REQUEST_SCHEMA = {
    'type': 'object',
    'properties': {
        'processing_limit': {'type': 'string'},
        'sheet_id': {'type': 'string'},
    },
    'required': [
        'processing_limit',
        'sheet_id',
    ],
}

# Batch size of how many search terms should be sent to Gemini in one request
BATCH_SIZE = 5


def _get_config_from_sheet(sheet_id: str) -> dict[str, str]:
  """Gets the Ads account config from the Google Sheet, and return the results.

  Args:
      sheet_id: The ID of the Google Sheet containing the config.

  Returns:
      Config from the Google Sheet.
  """
  logger.info('Getting config from sheet: %s', sheet_id)
  credentials, _ = google.auth.default(scopes=SCOPES)
  sheets_service = discovery.build(
      serviceName='sheets',
      version='v4',
      credentials=credentials,
      cache_discovery=False,
  )
  sheet = sheets_service.spreadsheets()

  system_instruction = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='thumbnail_age_system_instruction')
      .execute()
      .get('values', [])[0][0]
  )
  prompt = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='thumbnail_age_evaluation_prompt')
      .execute()
      .get('values', [])[0][0]
  )
  config = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='configuration')
      .execute()
      .get('values', [])
  )

  return {
      'system_instruction': system_instruction,
      'prompt': prompt,
      'settings': {item[0]: item[1] for item in config if len(item) >= 2},
  }


def _publish_data_as_batch(
    project_id: str,
    topic_id: str,
    messages_to_publish: list[dict[str, list[str]]],
) -> None:
  """Publishes multiple messages to a Pub/Sub topic with batch settings.

  Args:
      project_id: The Google Cloud Project with the pub/sub topic in.
      topic_id: The name of the topic to publish the message to.
      messages_to_publish: The messages to publish to the topic.
  """

  batch_settings = pubsub_v1.types.BatchSettings(
      max_messages=100
  )
  publisher = pubsub_v1.PublisherClient(batch_settings)
  topic_path = publisher.topic_path(project_id, topic_id)
  publish_futures = []

  # Resolve the publish future in a separate thread.
  def callback(
      data: bytes,
      current: int,
      total: int,
      topic_path: str,
      future: futures.Future[str],
  ) -> None:

    # Check for and log the exception for each dispatched message, this doesn't
    # block other threads.
    if future.exception():
      logger.info(
          'Failed to publish %s to %s, exception:\n%s.',
          data.decode('UTF-8'),
          topic_path,
          str(future.exception()),
      )
    else:
      logger.info(
          'Message %s (%d/%d) published to %s.',
          data.decode('UTF-8'),
          current,
          total,
          topic_path,
      )

  for current, data_item in enumerate(messages_to_publish):
    message_str = json.dumps(data_item)
    data = message_str.encode('utf-8')
    publish_future = publisher.publish(topic_path, data)
    publish_future.add_done_callback(
        functools.partial(
            callback, data, current + 1, len(messages_to_publish), topic_path
        )
    )
    publish_futures.append(publish_future)

  futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

  logger.info(
      'Published %d messages as a batch to %s.',
      len(messages_to_publish),
      topic_path,
  )


def run(sheet_id: str, processing_limit: int) -> None:
  """Gets new videos from BigQuery table and sends them for scoring.

  Args:
      sheet_id: The ID of the Google Sheet containing the config.
      processing_limit: The maximum number of records to process.
  """

  # create the prompt
  config = _get_config_from_sheet(sheet_id)

  logger.debug('System Instruction: %s', config['system_instruction'])
  logger.debug('Prompt: %s', config['prompt'])
  logger.debug('Settings: %s', config['settings'])

  if config['settings']['use_gemini_to_evaluate_age'] != 'Enabled':
    logger.warning(
        'Age evaluation is disabled, stopping execution.\n\nTo enable it,'
        ' change the value of "use_gemini_to_evaluate_age" in the Google Sheet'
        ' to "Enabled".\n\nTo prevent this warning from appearing again,'
        ' disable the Cloud Scheduler job that triggers this function.'
    )
    return

  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client()
  query = f"""
    SELECT
      DISTINCT video_id,
    FROM
      `{GOOGLE_CLOUD_PROJECT}.{BQ_SOURCE_DATASET}.{BQ_SOURCE_TABLE}` st
    WHERE NOT EXISTS (
      SELECT 1
        FROM `{GOOGLE_CLOUD_PROJECT}.{BQ_TARGET_DATASET}.{BQ_TARGET_TABLE}` tt
      WHERE 1 = 1
      AND tt.video_id = st.video_id
    )
    LIMIT {processing_limit}
  """
  logger.info('Query: %s', query)
  data = client.query(query).to_dataframe()

  if data.empty:
    logger.info('No thumbnails to process.')
    return

  data = data[[
      'video_id'
  ]].to_dict(orient='records')

  logger.info(
      '%d new video_id(s) to be dispatched for scoring.',
      len(data),
  )
  logger.debug(
      '%d new video_id(s) to be dispatched for scoring: %s',
      len(data),
      json.dumps(data, indent=2),
  )

  messages_to_publish = []
  for i in range(0, len(data), BATCH_SIZE):
    messages_to_publish.append({
        'system_instruction': config['system_instruction'],
        'prompt': config['prompt'],
        'batch_id': 'placeholder',
        'batch_part': str(int(i / BATCH_SIZE) + 1),
        'total_batch_parts': str(math.ceil(len(data) / BATCH_SIZE)),
        'videos': data[i : i + BATCH_SIZE],
    })

  logger.debug(
      '%d messages to be published: %s',
      len(messages_to_publish),
      json.dumps(messages_to_publish, indent=2),
  )

  logger.info(
      '%d messages to be published.',
      len(messages_to_publish),
  )

  _publish_data_as_batch(
      project_id=GOOGLE_CLOUD_PROJECT,
      topic_id=PUBSUB_TARGET_TOPIC,
      messages_to_publish=messages_to_publish,
  )


@functions_framework.http
def main(request: flask.Request) -> flask.Response:
  """The entry point: extract the data from the payload and starts the job.

  The request payload must match the request_schema object above.

  Args:
      request (flask.Request): HTTP request object.

  Returns:
      The flask response.
  """
  logger.info('Message dispatch started.')

  request_json = request.get_json()
  logger.info('JSON payload: %s', request_json)
  response = {}
  try:
    jsonschema.validate(instance=request_json, schema=REQUEST_SCHEMA)
  except jsonschema.exceptions.ValidationError as err:
    logger.error('Invalid request payload: %s', err)
    response['status'] = 'Failed'
    response['message'] = err.message
    return flask.Response(
        flask.json.dumps(response), status=400, mimetype='application/json'
    )

  run(
      processing_limit=int(request_json['processing_limit']),
      sheet_id=request_json['sheet_id'],
  )

  response['status'] = 'Success'
  response['message'] = 'Message dispatch finished.'

  logger.info('Message dispatch finished.')

  return flask.Response(
      flask.json.dumps(response), status=200, mimetype='application/json'
  )
