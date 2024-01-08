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
"""Pull YouTube data for the placements in the Google Ads report."""
import base64
import datetime
import json
import logging
import math
import os
import sys
from typing import Any, Dict, List

from google.cloud import bigquery
from googleapiclient import discovery
import jsonschema
import numpy as np
import pandas as pd

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The bucket to write the data to
GCS_DATA_BUCKET = os.environ.get('VID_EXCL_GCS_DATA_BUCKET')
# The name of the BigQuery Dataset
BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')

# Maximum number of channels per YouTube request. See:
# https://developers.google.com/youtube/v3/docs/channels/list
CHUNK_SIZE = 50

# The schema of the JSON in the event payload
message_schema = {
    'type': 'object',
    'properties': {
        'date_partition': {'type': 'string'},
    },
    'required': [
        'date_partition',
    ],
}
# BQ Table names to store the Youtube data in. This is not
# expected to be configurable and so is not exposed as an environmental variable
BQ_SOURCE_TABLE_NAME = 'GoogleAdsReportChannel'
BQ_TARGET_TABLE_NAME = 'YouTubeChannel'


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

  run(message_json.get('date_partition'))

  logger.info('YouTube channel service finished.')


def run(date_partition: str) -> None:
  """Orchestration to pull YouTube data and output it to BigQuery.

  Args:
      date_partition: The name of the newly created account report file.
  """
  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client()

  query = f'''
    SELECT DISTINCT channel_id
    FROM {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.{BQ_SOURCE_TABLE_NAME}
    WHERE TIMESTAMP_TRUNC(datetime_updated, DAY) = TIMESTAMP("{date_partition}")
    AND channel_id NOT IN (
        SELECT DISTINCT channel_id
        FROM {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.{BQ_TARGET_TABLE_NAME})
  '''
  # to_dataframe seems to be the fastest method to get a large amount of data
  # from BQ.
  channel_ids = client.query(query).to_dataframe()

  if channel_ids.empty:
    logger.info('No new channels to process.')
    return

  channel_ids = channel_ids['channel_id'].tolist()
  logger.info(
      '%d new video_id(s) to get YouTube metadata for.',
      len(channel_ids),
  )
  youtube_data = _get_channel_details(channel_ids)
  _write_results_to_bq(
      client=client,
      data=youtube_data,
      table_name=BQ_TARGET_TABLE_NAME
  )
  logger.info('All new videos processed.')


def _get_channel_details(channel_ids: set[str]) -> pd.DataFrame:
  """Pulls information on each of the channels provide from the YouTube API.

  The YouTube API only allows pulling up to 50 channels in each request, so
  multiple requests have to be made to pull all the data. See the docs for
  more details:
  https://developers.google.com/youtube/v3/docs/channels/list

  Args:
      channel_ids: The channel IDs to pull the info on from YouTube.

  Returns:
      A dataframe containing the YouTube channel information.
  """
  logger.info('Getting YouTube data for channel IDs')
  chunks = split_list_to_chunks(list(channel_ids), CHUNK_SIZE)
  number_of_chunks = len(chunks)

  logger.info('Connecting to the youtube API')
  youtube = discovery.build('youtube', 'v3')
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
  logger.info('YouTube channel info complete')

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
        int(channel.get('statistics').get('viewCount', '0')),
        int(channel.get('statistics').get('videoCount', '0')),
        int(channel.get('statistics').get('subscriberCount', '0')),
        channel.get('snippet').get('title', ''),
        channel.get('snippet').get('country', ''),
        topic_categories,
        clean_topics,
    ])
  return data


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
      write_disposition='WRITE_APPEND',
  )

  job = client.load_table_from_dataframe(
      dataframe=data, destination=destination, job_config=job_config
  )
  job.result()

  logger.info('Wrote %d records to table %s.', len(data.index), destination)
