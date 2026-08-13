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

"""Fetch the Google Ads configs and push them to pub/sub."""

import os
from typing import Any

import flask
import functions_framework
import google.auth
from googleapiclient import discovery
import jsonschema
from vet_common.ids import sanitize_gads_id
from vet_common.logging import get_service_logger
from vet_common.pubsub import publish_batch

logger = get_service_logger()

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
ACCOUNT_PUBSUB_TOPIC = os.environ.get('VET_GOOGLE_ADS_ACCOUNT_TOPIC')

# The access scopes used in this function
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The schema of the JSON in the request
REQUEST_SCHEMA = {
    'type': 'object',
    'properties': {
        'sheet_id': {'type': 'string'},
    },
    'required': [
        'sheet_id',
    ],
}


def _get_config_from_sheet(sheet_id: str) -> list[dict[str, Any]]:
  """Gets the Ads account config from the Google Sheet, and return the results.

  Args:
      sheet_id: The ID of the Google Sheet containing the config.

  Returns:
      A row for each account a report needs to be run for.

      [
          {
              'sheet_id': 'abcdefghijklmnop-mk',
              'customer_id': '1234567890',
              'mcc_for_exclusions': '5555555555',
              'lookback_days': 90,
              'gads_filters': 'clicks > 10',
              'settings': {
                  'foo': 'bar',
                  'baz': 'qux',
              }
          },
          ...
      ]
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
  lookback_days_values = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='google_ads_lookback_days')
      .execute()
      .get('values', [['1']])
  )
  lookback_days = (
      lookback_days_values[0][0]
      if lookback_days_values and lookback_days_values[0]
      else '1'
  )
  config = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='configuration')
      .execute()
      .get('values', [])
  )

  gads_filters_str = _gads_filters_to_gaql_string(gads_filters)

  logger.info('Returned %i customer_ids', len(customer_ids))
  account_configs = []
  for row in customer_ids:
    if len(row) < 2:
      continue
    customer_id = row[0]
    is_enabled = row[1]
    mcc_for_exclusions = row[2] if len(row) >= 3 else ''
    if is_enabled == 'Enabled':
      account_configs.append({
          'sheet_id': sheet_id,
          'customer_id': sanitize_gads_id(customer_id),
          'mcc_for_exclusions': sanitize_gads_id(mcc_for_exclusions),
          'lookback_days': int(lookback_days),
          'gads_filters': gads_filters_str,
          'settings': {item[0]: item[1] for item in config if len(item) >= 2},
      })
    else:
      logger.info('Ignoring disabled row: %s', customer_id)

  logger.info('Account configs:')
  logger.info(account_configs)
  return account_configs


def _gads_filters_to_gaql_string(config_filters: list[list[str]]) -> str:
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
    if len(row) >= 3:
      conditions.append(f'metrics.{row[0]} {row[1]} {row[2]}')
  return ' AND '.join(conditions)


def run(sheet_id: str) -> None:
  """Orchestration for the function.

  Args:
      sheet_id: the ID of the Google Sheet containing the config.
  """
  logger.info('Running Google Ads account script')
  publish_batch(
      project_id=GOOGLE_CLOUD_PROJECT,
      topic_id=ACCOUNT_PUBSUB_TOPIC,
      messages=_get_config_from_sheet(sheet_id),
      logger=logger,
  )
  logger.info('Done.')


@functions_framework.http
def main(request: flask.Request) -> flask.Response:
  """The entry point: extract the data from the payload and starts the job.

  The request payload must match the request_schema object above.

  Args:
      request (flask.Request): HTTP request object.

  Returns:
      The flask response.
  """
  logger.info('Google Ads Account dispatch triggered.')

  request_json = request.get_json(silent=True)
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

  try:
    run(request_json['sheet_id'])
  except Exception as err:  # pylint: disable=broad-except
    logger.error('Failed to run Google Ads account script: %s', err)
    response['status'] = 'Failed'
    response['message'] = str(err)
    return flask.Response(
        flask.json.dumps(response), status=500, mimetype='application/json'
    )

  response['status'] = 'Success'
  response['message'] = 'Google Ads Account dispatch finished.'

  logger.info('Google Ads Account dispatch finished.')

  return flask.Response(
      flask.json.dumps(response), status=200, mimetype='application/json'
  )
