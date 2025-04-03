# Copyright 2024 Google LLC
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
import json
import logging
import os
import sys

from google.ads.googleads import client as googleads_client
from google.cloud import bigquery
from google.protobuf import json_format
import jsonschema
import pandas as pd

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project containing the GCS bucket
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')

# The schema of the JSON in the event payload
message_schema = {
    'type': 'object',
    'properties': {
        'customer_id': {'type': 'string'},
    },
    'required': [
        'customer_id',
    ],
}

GOOGLE_ADS_CLIENT_VERSION = 'v18'
# BQ Table name to store the Google Ads video placement report. This is not
# expected to be configurable and so is not exposed as an environmental variable
BIGQUERY_TABLE_NAME = 'GoogleAdsExclusions'


def main(event: str, context: str) -> None:
  """The entry point: extracts the data from the payload and starts the job.

  The pub/sub message must match the message_schema object above.

  Args:
    event: A dictionary representing the event data payload.
    context: An object containing metadata about the event.
  """
  del context
  logger.info('Google Ads Exclusions Service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  jsonschema.validate(instance=message_json, schema=message_schema)

  run(message_json.get('customer_id'))

  logger.info('Google Ads Exclusions Service finished.')


def run(customer_id: str) -> None:
  """Starts the job to run the report from Google Ads and write it to BQ.

  Args:
    customer_id: the customer ID to fetch the Google Ads data for.
  """
  logger.info('Starting job to fetch Google Ads exclusion data for %s',
              customer_id)

  data = _get_exclusions(customer_id)

  if data.empty:
    logger.info('No exclusions found.')
    return

  bq_client = bigquery.Client()
  _write_results_to_bq(
      client=bq_client,
      data=data,
      table_name=BIGQUERY_TABLE_NAME
  )

  logger.info('Job complete')


def _get_exclusions(customer_id: str) -> pd.DataFrame:
  """Runs the group placement report in Google Ads & returns a Dataframe of the data.

  Args:
    customer_id: The customer ID to fetch the Google Ads data for.

  Returns:
    A Pandas DataFrame containing the report results.
  """
  logger.info('Getting report stream for %s', customer_id)
  client = googleads_client.GoogleAdsClient.load_from_env(
      version=GOOGLE_ADS_CLIENT_VERSION
  )
  ga_service = client.get_service('GoogleAdsService')

  query = """
    SELECT
    shared_criterion.type,
    shared_criterion.youtube_video.video_id,
    shared_criterion.youtube_channel.channel_id,
    shared_set.name,
    shared_set.type,
    shared_set.status
    FROM shared_criterion
    WHERE shared_criterion.type IN
    ('YOUTUBE_CHANNEL','YOUTUBE_VIDEO')
    AND shared_set.status = 'ENABLED'
  """

  stream = ga_service.search_stream(customer_id=customer_id, query=query)

  logger.info('Processing response stream')
  shared_criterions = [
      pd.DataFrame(
          columns=[
              'sharedCriterion.type',
              'sharedCriterion.youtubeChannel.channelId',
              'sharedCriterion.youtubeVideo.videoId',
              'sharedSet.name',
              'sharedSet.status',
          ]
      )
  ]
  for batch in stream:
    dictobj = json_format.MessageToDict(batch)
    shared_criterions.append(
        pd.json_normalize(dictobj, record_path=['results'])
    )

  exclusions = pd.concat(shared_criterions, ignore_index=True)

  exclusions['customer_id'] = customer_id
  exclusions['datetime_updated'] = pd.Timestamp.now().floor('S')

  exclusions.loc[
      exclusions['sharedCriterion.type'] == 'YOUTUBE_CHANNEL', 'id'
  ] = exclusions['sharedCriterion.youtubeChannel.channelId']

  exclusions.loc[
      exclusions['sharedCriterion.type'] == 'YOUTUBE_VIDEO', 'id'
  ] = exclusions['sharedCriterion.youtubeVideo.videoId']

  exclusions.rename(
      columns={
          'sharedSet.name': 'exclusion_list',
          'sharedCriterion.type': 'exclusion_type',
      },
      inplace=True,
  )

  return exclusions[[
      'id',
      'exclusion_list',
      'exclusion_type',
      'customer_id',
      'datetime_updated',
  ]]


def _write_results_to_bq(
    client: bigquery.Client,
    data: pd.DataFrame,
    table_name: str,
) -> None:
  """Writes the YouTube dataframe to BQ.

  Args:
      client: The BigQuery client.
      data: The dataframe based on the YouTube data.
      table_name: The name of the BQ table.
  """

  destination = '.'.join([GOOGLE_CLOUD_PROJECT, BIGQUERY_DATASET, table_name])
  job_config = bigquery.LoadJobConfig(
      write_disposition='WRITE_TRUNCATE',
  )

  job = client.load_table_from_dataframe(
      dataframe=data, destination=destination, job_config=job_config
  )
  job.result()

  logger.info('Wrote %d records to table %s.', len(data.index), destination)
