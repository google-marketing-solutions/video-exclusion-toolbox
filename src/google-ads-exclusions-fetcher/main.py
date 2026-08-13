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
"""Output the placement report from Google Ads to BigQuery."""
import base64
import datetime
import io
import json
import logging
import os
import sys
from typing import Any

import functions_framework
from google.ads.googleads import client as gads_client
from google.ads.googleads import errors as gads_errors
from google.api_core import exceptions as api_exceptions
import google.auth
from google.cloud import bigquery
from google.cloud import logging as cloud_logging
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

logging.getLogger('google.ads.googleads.client').setLevel(logging.WARNING)
logger = logging.getLogger(CLOUD_FUNCTION_NAME)

# General config
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The Google Ads config
GOOGLE_ADS_CLIENT_VERSION = os.environ.get('GOOGLE_ADS_CLIENT_VERSION')
LOGIN_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
DEVELOPER_TOKEN = os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN')
USE_PROTO_PLUS = os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS')
# BQ config
BIGQUERY_TARGET_DATASET = os.environ.get('VET_BIGQUERY_TARGET_DATASET')
BIGQUERY_TARGET_TABLE = os.environ.get('VET_BIGQUERY_TARGET_TABLE')

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
]


EXPECTED_LIST_NAMES = (
    '[video exclusion toolbox] - {exclusion_list_mcc_id} - bad videos',
    '[video exclusion toolbox] - {exclusion_list_mcc_id} - bad channels',
    (
        '[video exclusion toolbox] - {exclusion_list_mcc_id} - bad channels'
        ' (because of videos)'
    ),
)

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

TABLE_SCHEMA = [
    {'name': 'id', 'mode': 'REQUIRED', 'type': 'STRING'},
    {'name': 'exclusion_type', 'mode': 'REQUIRED', 'type': 'STRING'},
    {'name': 'customer_id', 'mode': 'REQUIRED', 'type': 'STRING'},
    {'name': 'exclusion_list', 'mode': 'REQUIRED', 'type': 'STRING'},
    {'name': 'exclusion_resource_name', 'mode': 'REQUIRED', 'type': 'STRING'},
    {'name': 'datetime_updated', 'mode': 'REQUIRED', 'type': 'TIMESTAMP'},
]


def _get_config_from_sheet(
    sheet_id: str, credentials: google.auth.credentials.Credentials
) -> list[dict[str, Any]]:
  """Gets the Ads account config from the Google Sheet, and return the results.

  Args:
      sheet_id: The ID of the Google Sheet containing the config.
      credentials: The credentials to use for the Google Sheets API.

  Returns:
      A row for each account a report needs to be run for.

      [
          {
              'sheet_id': 'abcdefghijklmnop-mk',
              'customer_id': '1234567890'
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
  logger.info('Getting config from sheet: %s.', sheet_id)
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
  lookback_days = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='google_ads_lookback_days')
      .execute()
      .get('values', [['1']])[0][0]
  )
  settings = (
      sheet.values()
      .get(spreadsheetId=sheet_id, range='configuration')
      .execute()
      .get('values', [])
  )

  gads_filters_query = ' AND '.join(
      [f'metrics.{row[0]} {row[1]} {row[2]}' for row in gads_filters]
  )

  logger.info('Returned %i customer_ids', len(customer_ids))
  account_configs = []
  for customer_id, is_enabled, mcc_for_exclusions in customer_ids:
    if is_enabled == 'Enabled':
      account_configs.append({
          'sheet_id': sheet_id,
          'customer_id': str(customer_id).replace('-', ''),
          'mcc_for_exclusions': str(mcc_for_exclusions).replace('-', ''),
          'lookback_days': int(lookback_days),
          'gads_filters': gads_filters_query,
          'settings': {item[0]: item[1] for item in settings if len(item) >= 2},
      })
    else:
      logger.info('Ignoring disabled row: %s', customer_id)

  logger.info('Account configs:')
  logger.info(account_configs)
  return account_configs


def _get_placement_exclusion_lists_for_mcc(
    client: gads_client.GoogleAdsClient,
    exclusion_list_mcc_id: str,
) -> dict[str, list[str]]:
  """Fetches the expected placement exclusion lists for an MCC.

  Args:
    client: An initialized GoogleAdsClient instance.
    exclusion_list_mcc_id: The Google Ads customer ID (MCC ID in this case).

  Returns:
    A dictionary mapping exclusion list names to lists of placement URLs.
  """
  get_placement_list_query = """
    SELECT
      shared_set.resource_name,
      shared_set.id,
      shared_set.name,
      shared_set.status
    FROM shared_set
    WHERE shared_set.type = 'NEGATIVE_PLACEMENTS'
    AND shared_set.status = 'ENABLED'
  """

  ga_service = client.get_service('GoogleAdsService')
  exclusion_placement_lists = []
  found_list_names = set()
  expected_list_names = {
      item.format(exclusion_list_mcc_id=exclusion_list_mcc_id)
      for item in EXPECTED_LIST_NAMES
  }
  logger.info(
      'Looking for exclusion lists in MCC %s: %s',
      exclusion_list_mcc_id,
      expected_list_names,
  )

  try:
    stream = ga_service.search_stream(
        customer_id=exclusion_list_mcc_id, query=get_placement_list_query
    )

    for batch in stream:
      for row in batch.results:
        shared_set = row.shared_set
        if shared_set.name in expected_list_names:
          logger.info('%s is an expected list.', shared_set.name)
          exclusion_placement_lists.append({
              'name': shared_set.name,
              'resource_name': shared_set.resource_name,
              'exclusion_list_mcc_id': exclusion_list_mcc_id,
          })
          found_list_names.add(shared_set.name)
    if not found_list_names:
      logger.error(
          'None of the expected exclusion lists (%s) were found for MCC %s.',
          expected_list_names,
          exclusion_list_mcc_id,
      )
      return {}

    missing_list_names = expected_list_names - found_list_names
    if missing_list_names:
      logger.warning(
          'Missing exclusion lists for MCC %s: %s',
          exclusion_list_mcc_id,
          missing_list_names,
      )
    return exclusion_placement_lists

  except gads_errors.GoogleAdsException as ex:
    logger.error(
        'Failed to fetch placement exclusion lists for %s: %s',
        exclusion_list_mcc_id,
        ex,
        exc_info=True,
    )
    return {}


def _get_exclusion_list_contents(
    client: gads_client.GoogleAdsClient,
    exclusion_list_mcc_id: str,
    exclusion_list_resource_name: str,
    exclusion_list_name: str,
) -> dict[str, list[str]]:
  """Fetches and prints the contents of placement exclusion lists for an MCC.

  Args:
    client: An initialized GoogleAdsClient instance.
    exclusion_list_mcc_id: The Google Ads customer ID (MCC ID in this case).
    exclusion_list_resource_name: The name of the exclusion list to fetch.
    exclusion_list_name: The name of the exclusion list to fetch.

  Returns:
    A dictionary mapping exclusion list names to lists of placement URLs.
  """
  get_placements_in_list_query = """
    SELECT
      shared_criterion.type,
      shared_criterion.criterion_id,
      shared_criterion.youtube_video.video_id,
      shared_criterion.youtube_channel.channel_id,
      shared_criterion.resource_name
    FROM shared_criterion
    WHERE shared_criterion.shared_set = '{shared_set_resource_name}'
    AND shared_criterion.type IN (YOUTUBE_CHANNEL, YOUTUBE_VIDEO)
  """
  ga_service = client.get_service('GoogleAdsService')
  placement_excusions = []
  logger.info(
      'Querying placements for list: %s (%s)',
      exclusion_list_name,
      exclusion_list_resource_name,
  )
  try:
    query = get_placements_in_list_query.format(
        shared_set_resource_name=exclusion_list_resource_name
    )
    logger.info('Query: %s', query)
    stream = ga_service.search_stream(
        customer_id=exclusion_list_mcc_id, query=query
    )
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for batch in stream:
      for row in batch.results:
        criterion = row.shared_criterion
        if criterion.type_ == 14:  # YOUTUBE_VIDEO
          exclusion_id = criterion.youtube_video.video_id
          exclusion_type = 'YOUTUBE_VIDEO'
        elif criterion.type_ == 15:  # YOUTUBE_CHANNEL
          exclusion_id = criterion.youtube_channel.channel_id
          exclusion_type = 'YOUTUBE_CHANNEL'
        else:
          logger.warning(
              'Unsupported criterion type: %s', criterion.type_
          )
          continue
        placement_excusions.append(
            {
                'id': exclusion_id,
                'customer_id': str(exclusion_list_mcc_id),
                'exclusion_list': exclusion_list_name,
                'exclusion_type': exclusion_type,
                'exclusion_resource_name': criterion.resource_name,
                'datetime_updated': now,
            }
        )

    if not placement_excusions:
      logger.info(
          'No placements found in list: %s', exclusion_list_resource_name
      )
      return []

    logger.info(
        'Found %d placements in list: %s',
        len(placement_excusions),
        exclusion_list_resource_name,
    )
    return placement_excusions
  except gads_errors.GoogleAdsException as ex:
    logger.error(
        'Failed to fetch placements for list %s for %s: %s',
        exclusion_list_resource_name,
        exclusion_list_mcc_id,
        ex,
        exc_info=True,
    )
    return []


def _write_data_to_bq(
    client: bigquery.Client,
    data_list: list[dict[str]],
    table_name: str,
    project_id: str,
    dataset_id: str,
) -> None:
  """Writes a list of dictionaries to BQ using an NDJSON load job.

  Args:
      client: The BigQuery client.
      data_list: The list of dictionaries representing the data rows.
      table_name: The base name of the BQ table (without project/dataset).
      project_id: The Google Cloud Project ID.
      dataset_id: The BigQuery Dataset ID.

  Raises:
      ValueError: If data_list is empty.
      MemoryError: If serialization fails due to memory limits.
      google.cloud.exceptions.GoogleCloudError: For BQ API errors.
  """
  if not data_list:
    logger.warning('No data provided to write to BigQuery.')
    return

  row_count = len(data_list)
  destination = f'{project_id}.{dataset_id}.{table_name}'
  logger.info(
      'Preparing to write %d records to table %s via NDJSON Load Job.',
      row_count,
      destination,
  )

  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition='WRITE_TRUNCATE',
      schema=TABLE_SCHEMA,
  )

  logger.debug('Serializing data to NDJSON...')
  try:
    ndjson_data = '\n'.join(
        json.dumps(row, ensure_ascii=False) for row in data_list
    )
    bytes_buffer = io.BytesIO(ndjson_data.encode('utf-8'))
  except MemoryError:
    logger.error(
        'MemoryError during NDJSON serialization. Increase function memory.'
    )
    raise

  logger.info(
      'Submitting Load Job %s for %s.',
      job_config.write_disposition,
      destination,
  )
  try:
    job = client.load_table_from_file(
        file_obj=bytes_buffer,
        destination=destination,
        job_config=job_config,
    )
    logger.info('Load job %s submitted. State: %s', job.job_id, job.state)

    logger.info('Waiting for job %s to complete.', job.job_id)
    job.result()
    logger.info('Job %s finished with state: %s', job.job_id, job.state)
    if job.errors:
      logger.error('Job %s completed with errors:', job.job_id)
      for error in job.errors:
        logger.error(' - %s: %s', error['reason'], error['message'])
    else:
      logger.info(
          'Wrote %d records to table %s.',
          row_count,
          destination
      )

  except api_exceptions.GoogleAPIError as e:
    logger.error('Failed to submit or execute BigQuery Load Job: %s', e)
    raise e


def run(sheet_id: str) -> None:
  """Starts the job to run the report from Google Ads and write it to BQ.

  Args:
      sheet_id: The ID of the Google Sheet containing the config.
  """
  logger.info('Authenticating.')
  credentials, _ = google.auth.default(scopes=SCOPES)

  # Get the config from the sheet.
  config = _get_config_from_sheet(sheet_id=sheet_id, credentials=credentials)
  logger.info('Config: %s', json.dumps(config, indent=2))

  if not config:
    logger.warning('No valid account configurations found in the sheet.')
    return

  # Get the MCCs to fetch exclusions for.
  mccs_for_exclusions = {
      item['mcc_for_exclusions']
      for item in config
      if item.get('mcc_for_exclusions')
  }
  logger.info('MCCs to fetch exclusions for: %s', mccs_for_exclusions)

  if not mccs_for_exclusions:
    logger.warning('No MCCs specified for fetching exclusions.')
    return

  try:
    logger.info('Creating Google Ads client for %s.', LOGIN_CUSTOMER_ID)
    client = gads_client.GoogleAdsClient(
        version=GOOGLE_ADS_CLIENT_VERSION,
        credentials=credentials,
        developer_token=DEVELOPER_TOKEN,
        login_customer_id=LOGIN_CUSTOMER_ID,
        use_proto_plus=USE_PROTO_PLUS,
    )
  except gads_errors.GoogleAdsException as e:
    logger.error('Failed to initialize Google Ads client: %s', e, exc_info=True)
    return

  # Get the exclusion lists for each MCC.
  exclusions_lists = []
  for exclusion_list_mcc_id in mccs_for_exclusions:
    logger.info('Fetching exclusion lists for MCC: %s', exclusion_list_mcc_id)
    exclusion_lists_for_mcc = _get_placement_exclusion_lists_for_mcc(
        client=client, exclusion_list_mcc_id=exclusion_list_mcc_id
    )
    if not exclusion_lists_for_mcc:
      logger.info('No exclusion lists found for MCC %s.', exclusion_list_mcc_id)
      continue

    logger.info(
        '%d exclusion lists found for MCC %s.',
        len(exclusion_lists_for_mcc),
        exclusion_list_mcc_id,
    )
    exclusions_lists.extend(exclusion_lists_for_mcc)

  if not exclusions_lists:
    logger.warning('No expected exclusion lists found for any MCCs.')
    return

  # Fetch the placements for each exclusion list.
  all_exclusions = []
  for exclusions_list in exclusions_lists:
    logger.info('Fetching exclusions for list: %s', exclusions_list['name'])
    exclusions_list_content = _get_exclusion_list_contents(
        client=client,
        exclusion_list_mcc_id=exclusions_list['exclusion_list_mcc_id'],
        exclusion_list_resource_name=exclusions_list['resource_name'],
        exclusion_list_name=exclusions_list['name'],
    )
    if not exclusions_list_content:
      logger.warning(
          'No placements found in list: %s', exclusions_list['name']
      )
      continue
    logger.info(
        'Found %d placements in list: %s',
        len(exclusions_list_content),
        exclusions_list['name'],
    )
    all_exclusions.extend(exclusions_list_content)

  if not all_exclusions:
    logger.warning('No placements found in any exclusion list.')
    return

  # Write the exclusions to BQ.
  logger.info('Connecting to BigQuery.')
  bq_client = bigquery.Client()
  _write_data_to_bq(
      client=bq_client,
      data_list=all_exclusions,
      table_name=BIGQUERY_TARGET_TABLE,
      project_id=GOOGLE_CLOUD_PROJECT,
      dataset_id=BIGQUERY_TARGET_DATASET,
  )


@functions_framework.cloud_event
def main(cloud_event: functions_framework.cloud_event) -> None:
  """The entry point: extract the data from the payload and starts the job.

  Handles errors internally to prevent Pub/Sub retries for failed messages.

  Args:
      cloud_event: A dictionary representing the event data payload.

  Raises:
      jsonschema.exceptions.ValidationError if the message from pub/sub is not
      what is expected before processing starts.
  """
  sheet_id_for_logging = 'unknown_sheet'
  data = None
  message_json = None

  try:
    logger.info('Google Ads Exclusions Fetcher started.')
    logger.debug('cloud_event: %s', cloud_event)
    data = base64.b64decode(cloud_event.data['message']['data']).decode('UTF-8')
    logger.debug('Decoded message: %s', data)
    message_json = json.loads(data)

    sheet_id_for_logging = message_json.get(
        'sheet_id', 'missing_sheet_id'
    )
    logger.info(
        '(Sheet: %s): JSON message: %s',
        sheet_id_for_logging,
        message_json,
    )

    jsonschema.validate(instance=message_json, schema=REQUEST_SCHEMA)

    run(
        sheet_id=message_json.get('sheet_id'),
    )

    logger.info(
        '(Sheet: %s): Google Ads Exclusions Fetcher finished successfully.',
        sheet_id_for_logging,
    )

  except jsonschema.exceptions.ValidationError as e:
    logger.error(
        '(Sheet: %s): Invalid Pub/Sub message schema: %s. Message: %s',
        sheet_id_for_logging,
        e,
        data,
        exc_info=True
    )
    return

  # Catch all exceptions to prevent Pub/Sub retries for failed messages.
  except Exception as e:  # pylint: disable=broad-except
    logger.error(
        '(Sheet: %s): Failed to process message. Error: %s. Message: %s',
        sheet_id_for_logging,
        e,
        message_json if message_json else data,
        exc_info=True
    )
    return
