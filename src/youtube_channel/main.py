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
"""Pull YouTube data for the placements in the Google Ads report."""
import base64
import datetime
import hashlib
import json
import logging
import math
import os
import sys
import time
from typing import Any, Dict, List

from google.api_core import exceptions
import google.auth
import google.auth.credentials
from google.cloud import bigquery
from googleapiclient import discovery
import jsonschema
import numpy as np
import pandas as pd
from utils import bq


logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The bucket to write the data to
GCS_DATA_BUCKET = os.environ.get('VID_EXCL_GCS_DATA_BUCKET')
# The name of the BigQuery Dataset
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')

# Optional variable to specify the problematic CSV characters. This is an env
# variable, so if any other characters come up they can be replaced in the
# Cloud Function UI without redeploying the solution.
CSV_PROBLEM_CHARACTERS_REGEX = os.environ.get(
    'VID_EXCL_CSV_PROBLEM_CHARACTERS', r'\$|\"|\'|\r|\n|\t|,|;|:'
)

# Maximum number of channels per YouTube request. See:
# https://developers.google.com/youtube/v3/docs/channels/list
CHUNK_SIZE = 50

# The access scopes used in this function
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The schema of the JSON in the event payload
message_schema = {
    'type': 'object',
    'properties': {
        'customer_id': {'type': 'string'},
        'blob_name': {'type': 'string'},
    },
    'required': [
        'blob_name',
        'customer_id',
    ],
}


def main(event: Dict[str, Any], context: Dict[str, Any]) -> None:
  """The entry point: extract the data from the payload and starts the job.

  The pub/sub message must match the message_schema object above.

  Args:
      event: A dictionary representing the event data payload.
      context: An object containing metadata about the event.

  Raises:
      jsonschema.exceptions.ValidationError if the message from pub/sub is not
      what is expected.
  """
  del context
  logger.info('YouTube channel service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=message_schema)

  run(message_json.get('blob_name'))

  logger.info('Done')


def run(blob_name: str) -> None:
  """Orchestration to pull YouTube data and output it to BigQuery.

  Args:
      blob_name: The name of the newly created account report file.
  """
  credentials = get_auth_credentials()

  # step 1: Pull list of YT channels IDs from the blob
  # Open blob and get specifc column
  data = pd.read_csv(f'gs://{GCS_DATA_BUCKET}/{blob_name}')
  channel_ids = data[['channel_id']].drop_duplicates()
  # for channel_ids not in BQ, run the following
  logger.info('Checking new channels')
  logger.info('Connecting to: %s BigQuery', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client(
      project=GOOGLE_CLOUD_PROJECT, credentials=credentials
  )
  temp_table = temp_table_from_csv(channel_ids, client)
  logger.info('Filtering previously processed channels.')
  query = f"""
            SELECT channel_id
            FROM
            `{BQ_DATASET}.{temp_table}`
            WHERE
            channel_id NOT IN (
                SELECT channel_id FROM `{BQ_DATASET}.YouTubeChannel`
            )
            """
  rows = client.query(query).result()
  channel_ids_to_check = set()
  for row in rows:
    channel_ids_to_check.add(row.channel_id)
  logger.info('Found %s new channel_ids', len(channel_ids_to_check))

  if channel_ids_to_check:
    get_youtube_dataframe(channel_ids_to_check, credentials)
  else:
    logger.info('No new channel IDs to process')
  logger.info('Done')


def get_youtube_dataframe(
    channel_ids: set[str],
    credentials: google.auth.credentials.Credentials,
) -> None:
  """Pulls information on each of the channels provide from the YouTube API.

  The YouTube API only allows pulling up to 50 channels in each request, so
  multiple requests have to be made to pull all the data. See the docs for
  more details:
  https://developers.google.com/youtube/v3/docs/channels/list

  Args:
      channel_ids: The channel IDs to pull the info on from YouTube.
      credentials: Google Auth credentials
  """
  logger.info('Getting YouTube data for channel IDs')
  chunks = split_list_to_chunks(list(channel_ids), CHUNK_SIZE)
  number_of_chunks = len(chunks)

  logger.info('Connecting to the youtube API')
  youtube = discovery.build('youtube', 'v3', credentials=credentials)
  all_channels = []
  for i, chunk in enumerate(chunks):
    logger.info('Processing chunk %s of %s', i + 1, number_of_chunks)
    chunk_list = list(chunk)
    request = youtube.channels().list(
        part='id, statistics, snippet, brandingSettings, topicDetails',
        id=chunk_list,
        maxResults=CHUNK_SIZE,
    )
    response = request.execute()
    channels = process_youtube_response(response, chunk_list)
    for channel in channels:
      all_channels.append(channel)
  youtube_df = pd.DataFrame(
      all_channels,
      columns=[
          'channel_id',
          'view_count',
          'video_count',
          'subscriber_count',
          'title',
          'country',
          'topic_categories',
          'clean_topics',
      ],
  )
  youtube_df['datetime_updated'] = datetime.datetime.now()
  youtube_df = sanitise_youtube_dataframe(youtube_df)
  write_results_to_bq(youtube_df, BQ_DATASET + '.YouTubeChannel')
  logger.info('YouTube channel info complete')


def get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials


def temp_table_from_csv(data: pd.DataFrame, client: bigquery.Client) -> str:
  """Creates a temporary BQ table to store video IDs for querying.

  Args:
      data: A dataframe containing the video IDs to be written.
      client: The object to connect to BQ.

  Returns:
      The name of the temporary table.
  """

  timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
  suffix = hashlib.sha256(timestamp.encode('utf-8')).hexdigest()[:6]
  table_name = '-'.join(['temp-channel-ids', suffix, timestamp])
  destination = '.'.join([GOOGLE_CLOUD_PROJECT, BQ_DATASET, table_name])
  logger.info('Creating a temporary table: %s', table_name)

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField(
              'channel_id', bigquery.enums.SqlTypeNames.STRING
          ),
      ],
      write_disposition='WRITE_TRUNCATE',
  )

  job = client.load_table_from_dataframe(
      dataframe=data, destination=destination, job_config=job_config
  )
  job.result()

  expiration = datetime.datetime.now(
      datetime.timezone.utc
  ) + datetime.timedelta(hours=1)

  # This is not elegant, but necessary. BQ seems to sometimes refuse to update
  # table's metadata shortly after creating it. Retrying after a few seconds
  # is a crude, but working workaround.
  try:
    table = client.get_table(destination)
    table.expires = expiration
    client.update_table(table, ['expires'])
  except exceptions.PreconditionFailed:
    logger.info(
        "Failed to update expiration for table '%s' wating 5 seconds and"
        ' retrying.',
        table_name
    )
    time.sleep(5)
    table = client.get_table(destination)
    table.expires = expiration
    client.update_table(table, ['expires'])

  logger.info("Table '%s' created.", table_name)

  return table_name


def sanitise_youtube_dataframe(youtube_df: pd.DataFrame) -> pd.DataFrame:
  """Takes the dataframe from YouTube and sanitises it to write as a CSV.

  Args:
      youtube_df: The dataframe containing the YouTube data.

  Returns:
      The YouTube dataframe but sanitised to be safe to write to a CSV.
  """
  youtube_df['view_count'] = youtube_df['view_count'].fillna(0)
  youtube_df['video_count'] = youtube_df['video_count'].fillna(0)
  youtube_df['subscriber_count'] = youtube_df['subscriber_count'].fillna(0)
  youtube_df = youtube_df.astype({
      'view_count': 'int',
      'video_count': 'int',
      'subscriber_count': 'int',
  })
  # remove problematic characters from the title field as the break BigQuery
  # even when escaped in the CSV
  youtube_df['title'] = youtube_df['title'].str.replace(
      CSV_PROBLEM_CHARACTERS_REGEX, '', regex=True
  )
  youtube_df['title'] = youtube_df['title'].str.strip()
  return youtube_df


def split_list_to_chunks(
    lst: List[Any], max_size_of_chunk: int
) -> List[np.ndarray]:
  """Splits the list into X chunks with the maximum size as specified.

  Args:
      lst: The list to be split into chunks.
      max_size_of_chunk: The maximum number of elements that should be in a
        chunk.

  Returns:
      A list containing numpy array chunks of the original list.
  """
  logger.info('Splitting list into chunks')
  num_of_chunks = math.ceil(len(lst) / max_size_of_chunk)
  chunks = np.array_split(lst, num_of_chunks)
  logger.info('Split list into %i chunks', num_of_chunks)
  return chunks


def process_youtube_response(
    response: Dict[str, Any], channel_ids: List[str]
) -> List[List[Any]]:
  """Processes the YouTube response to extract the required information.

  Args:
      response: The YouTube channels list response.
          https://developers.google.com/youtube/v3/docs/channels/list#response
      channel_ids: A list of the channel IDs passed in the request.

  Returns:
      A list of dicts where each dict represents data from one channel.
  """
  logger.info('Processing youtube response')
  data = []
  if response.get('pageInfo').get('totalResults') == 0:
    logger.warning('The YouTube response has no results: %s', response)
    logger.warning(channel_ids)
    return data

  for channel in response['items']:
    topic_details = channel.get('topicDetails')
    if topic_details:
      topic_categories = topic_details.get('topicCategories', '')
    else:
      topic_categories = ''
    clean_topics = []
    for topic in topic_categories:
      clean_topics.append(topic.split('https://en.wikipedia.org/wiki/')[1])
    data.append([
        channel.get('id'),
        channel.get('statistics').get('viewCount', None),
        channel.get('statistics').get('videoCount', None),
        channel.get('statistics').get('subscriberCount', None),
        channel.get('snippet').get('title', ''),
        channel.get('snippet').get('country', ''),
        topic_categories,
        clean_topics,
    ])
  return data


def write_results_to_bq(
    youtube_df: pd.DataFrame, table_id: str
) -> None:
  """Writes the YouTube dataframe to BQ.

  Args:
      youtube_df: The dataframe based on the YouTube data.
      table_id: The id of the BQ table.
  """
  logger.info('Writing results to BQ: %s', table_id)
  number_of_rows = len(youtube_df.index)
  logger.info('There are %s rows', number_of_rows)
  if number_of_rows > 0:
    bq.load_to_bq_from_df(
        df=youtube_df, table_id=table_id
    )
    logger.info('YT data added to BQ table')
  else:
    logger.info('There is nothing to write to BQ')
