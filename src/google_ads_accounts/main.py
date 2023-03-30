# Copyright 2023 Google LLC
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
"""Fetch the Google Ads configs and push them to pub/sub."""
import logging
import os
import sys
from typing import Any, List, Dict
import flask
import google.auth
from googleapiclient.discovery import build
import jsonschema
from utils import pubsub


logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the pub/sub topic
YTAD_ADS_REPORT_VIDEO_PUBSUB_TOPIC = os.environ.get(
    'YTAD_ADS_REPORT_VIDEO_PUBSUB_TOPIC'
)
YTAD_ADS_REPORT_CHANNEL_PUBSUB_TOPIC = os.environ.get(
    'YTAD_ADS_REPORT_CHANNEL_PUBSUB_TOPIC'
)
YTAD_ADS_EXCLUSIONS_PUBSUB_TOPIC = os.environ.get(
    'YTAD_ADS_EXCLUSIONS_PUBSUB_TOPIC'
)

# The access scopes used in this function
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The schema of the JSON in the request
request_schema = {
    'type': 'object',
    'properties': {
        'sheet_id': {'type': 'string'},
    },
    'required': [
        'sheet_id',
    ],
}


def main(request: flask.Request) -> flask.Response:
  """The entry point: extract the data from the payload and starts the job.

  The request payload must match the request_schema object above.

  Args:
      request (flask.Request): HTTP request object.

  Returns:
      The flask response.
  """
  logger.info('Google Ads Account Service triggered.')
  request_json = request.get_json()
  logger.info('JSON payload: %s', request_json)
  response = {}
  try:
    jsonschema.validate(instance=request_json, schema=request_schema)
  except jsonschema.exceptions.ValidationError as err:
    logger.error('Invalid request payload: %s', err)
    response['status'] = 'Failed'
    response['message'] = err.message
    return flask.Response(
        flask.json.dumps(response), status=400, mimetype='application/json'
    )

  run(request_json['sheet_id'])

  response['status'] = 'Success'
  response['message'] = 'Downloaded data successfully'
  return flask.Response(
      flask.json.dumps(response), status=200, mimetype='application/json'
  )


def run(sheet_id: str) -> None:
  """Orchestration for the function.

  Args:
      sheet_id: the ID of the Google Sheet containing the config.
  """
  logger.info('Running Google Ads account script')
  account_configs = get_config_from_sheet(sheet_id)
  send_messages_to_pubsub(account_configs)
  logger.info('Done.')


def get_config_from_sheet(sheet_id: str) -> List[Dict[str, Any]]:
  """Gets the Ads account config from the Google Sheet, and return the results.

  Args:
      sheet_id: The ID of the Google Sheet containing the config.

  Returns:
      A row for each account a report needs to be run for.

      [
          {
              'sheet_id': 'abcdefghijklmnop-mk',
              'customer_id': '1234567890'
              'lookback_days': 90,
              'gads_filters': 'metrics.clicks > 10',
          },
          ...
      ]
  """
  logger.info('Getting config from sheet: %s', sheet_id)
  credentials, _ = google.auth.default(scopes=SCOPES)
  sheets_service = build('sheets', 'v4', credentials=credentials)
  sheet = sheets_service.spreadsheets()

  customer_ids = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='google_ads_customer_ids')
      .execute()
      .get('values', [])
  )
  gads_filters = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='google_ads_filters')
      .execute()
      .get('values', [])
  )
  lookback_days = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='google_ads_lookback_days')
      .execute()
      .get('values', [['30']])[0][0]
  )

  gads_filters_str = gads_filters_to_gaql_string(gads_filters)

  logger.info('Returned %i customer_ids', len(customer_ids))
  account_configs = []
  for customer_id, is_enabled in customer_ids:
    if is_enabled == 'Enabled':
      account_configs.append({
          'sheet_id': sheet_id,
          'customer_id': customer_id,
          'lookback_days': int(lookback_days),
          'gads_filters': gads_filters_str,
      })
    else:
      logger.info('Ignoring disabled row: %s', customer_id)

  logger.info('Account configs:')
  logger.info(account_configs)
  return account_configs


def gads_filters_to_gaql_string(config_filters: List[List[str]]) -> str:
  """Turns the Google Ads filters into a GAQL compatible string.

  The config sheet has the filters in a list of lists, these need to be
  combined, so they can be used in a WHERE clause in the GAQL that is passed
  to Google Ads. See:
  https://developers.google.com/google-ads/api/docs/query/overview

  Each row is "AND" together.

  Args:
      config_filters: The filters from the Google Sheet.

  Returns:
      A string that can be used in the WHERE statement of the Google Ads Query
      Language.
  """
  conditions = []
  for row in config_filters:
    conditions.append(f'metrics.{row[0]} {row[1]} {row[2]}')
  return ' AND '.join(conditions)


def send_messages_to_pubsub(messages: List[Dict[str, Any]]) -> None:
  """Pushes each of the messages to the pubsub topic.

  Args:
      messages: The list of messages to push to pubsub.
  """
  logger.info('Sending messages to pubsub')
  logger.info('Messages: %s', messages)
  pubsub.send_dicts_to_pubsub(
      messages=messages,
      topic=YTAD_ADS_REPORT_VIDEO_PUBSUB_TOPIC,
      gcp_project=GOOGLE_CLOUD_PROJECT,
  )
  pubsub.send_dicts_to_pubsub(
      messages=messages,
      topic=YTAD_ADS_REPORT_CHANNEL_PUBSUB_TOPIC,
      gcp_project=GOOGLE_CLOUD_PROJECT,
  )
  pubsub.send_dicts_to_pubsub(
      messages=messages,
      topic=YTAD_ADS_EXCLUSIONS_PUBSUB_TOPIC,
      gcp_project=GOOGLE_CLOUD_PROJECT,
  )
  logger.info('All messages published')
