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
"""Uploads new exclusions to shared placement exclusions list."""

import logging
import os
import sys
from google.ads.googleads.client import GoogleAdsClient
import google.auth
import google.auth.credentials
from google.cloud import bigquery
from google.protobuf import json_format
import pandas as pd

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
]


def main(request: str) -> str:
  """Entry point, kicks off the job to upload exclusions to Google Ads.

  Args:
    request: A request containing a customer ID and shared set name (exclusion
      list name) to apply the exclusions.

  Returns:
    "OK" string.
  """
  request_data = request.get_json(silent=True)
  customer_id = request_data['customer_id']
  shared_set_name = request_data['shared_set_name']
  logger.info(
      'Starting job to fetch Google Ads exclusion data for %s', customer_id
  )

  run(shared_set_name=shared_set_name, customer_id=customer_id)

  return 'OK'


def run(shared_set_name: str, customer_id: str) -> None:
  """Orchestrates adding the new exclusions to Google Ads.

  Args:
    shared_set_name: Name of the exclusion list.
    customer_id: the customer ID to fetch the Google Ads data for.
  """
  bq_client = bigquery.Client(
      project=GOOGLE_CLOUD_PROJECT, credentials=get_auth_credentials()
  )
  gads_client = GoogleAdsClient.load_from_env(version='v14')

  _write_results_to_bq(
      client=bq_client,
      data=get_exclusions(client=gads_client, customer_id=customer_id),
      table_name='GoogleAdsExclusions',
  )

  exclusions_to_upload = get_exclusions_to_upload(
      client=bq_client, shared_set_name=shared_set_name
  )

  if exclusions_to_upload['videos'] or exclusions_to_upload['channels']:
    shared_set_name_to_id = get_exclusion_list_name_and_ids(
        client=gads_client, customer_id=customer_id
    )

    upload_exclusions(
        client=gads_client,
        customer_id=customer_id,
        exclusions_to_upload=exclusions_to_upload,
        shared_set_id=shared_set_name_to_id[shared_set_name],
    )

    _write_results_to_bq(
        client=bq_client,
        data=get_exclusions(client=gads_client, customer_id=customer_id),
        table_name='GoogleAdsExclusions'
    )
  else:
    logger.info('No new videos/channels to upload. Job complete.')


def get_exclusions_to_upload(
    client: bigquery.Client, shared_set_name: str
) -> dict[list[str]]:
  """Gets the list of videos/channels to exclude.

  Args:
    client: BigQuery client.
    shared_set_name: Name of the exclusion list.

  Returns:
    A dictionary with two lists: videos to exclude and channels to exclude.
  """
  query = f"""
            SELECT video_id as id, 'video_id' as type
            FROM
              {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.VideosToExclude
            WHERE
            video_id NOT IN (
                SELECT id FROM
                  {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.GoogleAdsExclusions
                WHERE exclusion_list = '{shared_set_name}')
            UNION ALL
            SELECT channel_id as id, 'channel_id' as type
            FROM
              {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.ChannelsToExclude
            WHERE
            channel_id NOT IN (
                SELECT id FROM
                  {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.GoogleAdsExclusions
                WHERE exclusion_list = '{shared_set_name}')
            """
  rows = client.query(query).result()
  ids_to_exclude = {}
  ids_to_exclude['videos'] = []
  ids_to_exclude['channels'] = []
  for row in rows:
    if row.type == 'video_id':
      ids_to_exclude['videos'].append(row.id)
    elif row.type == 'channel_id':
      ids_to_exclude['channels'].append(row.id)
  logger.info('Found %d new videos, %d new channels to exclude in %s',
              len(ids_to_exclude['videos']),
              len(ids_to_exclude['channels']),
              shared_set_name)
  return(ids_to_exclude)


def get_exclusions(client: GoogleAdsClient, customer_id: str) -> pd.DataFrame:
  """Gets the exclusions for the given customer ID.

  Args:
    client: Google Ads client.
    customer_id: The customer ID to fetch the Google Ads data for.

  Returns:
    A Pandas DataFrame containing the report results.
  """
  logger.info('Getting report stream for %s', customer_id)
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


def get_exclusion_list_name_and_ids(
    client: GoogleAdsClient, customer_id: str
) -> dict[str, str]:
  """Gets exclusion lists names and IDs for the specific customer ID.

  Args:
    client: The Google Ads client to use.
    customer_id: The customer ID to fetch the exclusion lists for.

  Returns:
    A dictionary of exclusion lists names to ids.
  """
  logger.info('Getting exclusion list details for %s', customer_id)
  ga_service = client.get_service('GoogleAdsService')

  query = """
    SELECT
    shared_set.id,
    shared_set.name
    FROM shared_set
    """

  stream = ga_service.search_stream(customer_id=customer_id, query=query)
  shared_set_name_to_id = {}
  for batch in stream:
    for row in batch.results:
      shared_set_name_to_id[row.shared_set.name] = row.shared_set.id
  return shared_set_name_to_id


def upload_exclusions(
    client: GoogleAdsClient,
    customer_id: str,
    exclusions_to_upload: list[str],
    shared_set_id: str,
) -> None:
  """Uploads new exclusions to placement exclusions list.

  Args:
    client: The Google Ads client to use.
    customer_id: The customer ID to upload the exclusions on.
    exclusions_to_upload: List of video/channel IDs to upload to the exclusion
      list.
    shared_set_id: The placement exclusion list ID to upload to.
  """
  service = client.get_service('SharedCriterionService')
  operations = []
  shared_set = f'customers/{customer_id}/sharedSets/{shared_set_id}'
  logger.info('Processing the %i placements', len(exclusions_to_upload))
  for placement in exclusions_to_upload['videos']:
    operation = client.get_type('SharedCriterionOperation')
    criterion = operation.create
    criterion.shared_set = shared_set
    criterion.youtube_video.video_id = placement
    operations.append(operation)
  for placement in exclusions_to_upload['channels']:
    operation = client.get_type('SharedCriterionOperation')
    criterion = operation.create
    criterion.shared_set = shared_set
    criterion.youtube_channel.channel_id = placement
    operations.append(operation)

  # Issues a mutate request to add the negative customer criteria.
  response = service.mutate_shared_criteria(
      customer_id=customer_id,
      operations=operations,
  )
  logger.info(
      'Added %d videos/channels to placement exclusion %s list',
      len(response.results),
      (shared_set_id),
  )


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


def get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  # Scopes include drive API here as one table is mirroring a Google Sheet
  credentials, _ = google.auth.default(scopes=SCOPES)
  return credentials
