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

"""Pull YouTube video data for the placements in the Google Ads report."""
import base64
import datetime
import json
import logging
import os
import sys
from typing import Any, Dict, List

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
# The name of the BigQuery Dataset
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')


# Maximum number of channels per YouTube request. See:
# https://developers.google.com/youtube/v3/docs/videos/list
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
BQ_SOURCE_TABLE_NAME = 'GoogleAdsReportVideo'
BQ_TARGET_TABLE_NAME = 'YouTubeVideo'


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
  # deleting context, as it's not required, but gets passed in along the "event"
  # data when the Cloud Function is triggered from Pub/Sub
  del context
  logger.info('YouTube video service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=message_schema)

  run(message_json.get('date_partition'))

  logger.info('YouTube video service finished.')


def run(date_partition: str) -> None:
  """Orchestration to pull YouTube data and output it to BigQuery.

  Args:
      date_partition: The date fo the partition with the latest data.
  """
  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client()

  query = f'''
      SELECT DISTINCT video_id FROM {GOOGLE_CLOUD_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE_NAME}
      WHERE TIMESTAMP_TRUNC(datetime_updated, DAY) = TIMESTAMP("{date_partition}")
      AND video_id NOT IN
        (SELECT DISTINCT video_id FROM {GOOGLE_CLOUD_PROJECT}.{BQ_DATASET}.{BQ_TARGET_TABLE_NAME})
  '''
  # to_dataframe seems to be the fastest method to get a large amount of data
  # from BQ.
  data = client.query(query).to_dataframe()

  if data.empty:
    logger.info('No new videos to process.')
  else:
    video_ids = data['video_id'].tolist()
    logger.info(
        '%d new video_id(s) to get YouTube metadata for.',
        len(video_ids),
    )
    _get_youtube_videos_dataframe(video_ids)
    logger.info('All new videos processed.')


def _get_youtube_videos_dataframe(
    video_ids: List[str],
) -> None:
  """Pulls information on each of the videos provided from the YouTube API.

  The YouTube API only allows pulling up to 50 videos in each request, so
  multiple requests have to be made to pull all the data. See the docs for
  more details:
  https://developers.google.com/youtube/v3/docs/channels/list

  Args:
      video_ids: The video IDs to pull the info on from YouTube.
  """
  logger.info('Getting YouTube data for %d video IDs.', len(video_ids))

  chunks = _split_list_to_chunks(video_ids, CHUNK_SIZE)
  number_of_chunks = len(chunks)

  logger.info('Connecting to the YouTube API.')
  youtube = discovery.build('youtube', 'v3')

  all_videos = []

  for i, chunk in enumerate(chunks):
    logger.info('Processing chunk %s of %s.', i + 1, number_of_chunks)
    chunk_list = list(chunk)
    request = youtube.videos().list(
        part='id,snippet,contentDetails,statistics',
        id=chunk_list,
        maxResults=CHUNK_SIZE,
    )
    response = request.execute()
    videos = _process_youtube_videos_response(response, chunk_list)
    for video in videos:
      all_videos.append(video)

  youtube_df = pd.DataFrame(
      all_videos,
      columns=[
          'video_id',
          'title',
          'description',
          'publishedAt',
          'channelId',
          'categoryId',
          'tags',
          'defaultLanguage',
          'duration',
          'definition',
          'licensedContent',
          'ytContentRating',
          'viewCount',
          'likeCount',
          'commentCount',
      ],
  )
  youtube_df['datetime_updated'] = datetime.datetime.now()
  _write_results_to_bq(
      youtube_df=youtube_df,
      table_id='.'.join(
          [GOOGLE_CLOUD_PROJECT, BQ_DATASET, BQ_TARGET_TABLE_NAME]
      ),
  )
  logger.info('YouTube Video info complete')


def _split_list_to_chunks(
    data: List[Any], max_size_of_chunk: int
) -> List[np.ndarray]:
  """Splits the list into X chunks with the maximum size as specified.

  Args:
      data: The list to be split into chunks.
      max_size_of_chunk: The maximum number of elements that should be in a
        chunk.

  Returns:
      A list containing numpy array chunks of the original list.
  """
  logger.info('Splitting data into chunks')
  num_of_chunks = (len(data) + max_size_of_chunk - 1) // max_size_of_chunk
  chunks = np.array_split(data, num_of_chunks)
  logger.info('Split list into %i chunks', num_of_chunks)
  return chunks


def _process_youtube_videos_response(
    response: Dict[str, Any], video_ids: List[str]
) -> List[List[Any]]:
  """Processes the YouTube response to extract the required information.

  Args:
      response: The YouTube video list response
          https://developers.google.com/youtube/v3/docs/videos/list#response.
      video_ids: A list of the video IDs passed in the request.

  Returns:
      A list of lists, each list contains parsed data from one video. Returns
      an empty list if there were no videos in the response.
  """
  logger.info('Processing youtube response')
  data = []
  if response.get('pageInfo').get('totalResults') == 0:
    logger.warning('The YouTube response has no results: %s', response)
    logger.warning(video_ids)
    return data

  for video in response['items']:
    data.append([
        video.get('id'),
        video['snippet'].get('title', ''),
        video['snippet'].get('description', None),
        pd.Timestamp(video['snippet'].get('publishedAt', None)),
        video['snippet'].get('channelId', None),
        int(video['snippet'].get('categoryId', '0')),
        video['snippet'].get('tags', None),
        video['snippet'].get('defaultLanguage', ''),
        video['contentDetails'].get('duration', ''),
        video['contentDetails'].get('definition', ''),
        video['contentDetails'].get('licensedContent', ''),
        video['contentDetails'].get('contentRating').get('ytRating', ''),
        int(video['statistics'].get('viewCount', '0')),
        int(video['statistics'].get('likeCount', '0')),
        int(video['statistics'].get('commentCount', '0')),
    ])
  return data


def _write_results_to_bq(
    youtube_df: pd.DataFrame, table_id: str
) -> None:
  """Writes the YouTube dataframe to BQ.

  Args:
      youtube_df: The dataframe based on the YouTube data.
      table_id: The id of the BQ table.
  """
  number_of_rows = len(youtube_df.index)
  logger.info('Writing %d rows to BQ table %s.', number_of_rows, table_id)
  if number_of_rows > 0:
    bq.load_to_bq_from_df(df=youtube_df, table_id=table_id)
    logger.info('Wrote %d rows to BQ table %s.', number_of_rows, table_id)
  else:
    logger.info('There is nothing to write to BQ.')
