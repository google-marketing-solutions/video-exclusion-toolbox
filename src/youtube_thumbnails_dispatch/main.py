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

"""Dispatch Youtube Video IDs for processing, one by one.

Retrieve Youtube Video IDs from a BQ table's partition, filters the ones that
need to be processed and dispatches them to a Pub/Sub topic.
"""
import base64
from concurrent import futures
import functools
import json
import logging
import os
import sys
from typing import Any, Dict

from google.cloud import bigquery
from google.cloud import pubsub_v1
import jsonschema

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The bucket to read the file with video IDs from.
THUMBNAIL_PROCESSING_TOPIC = os.environ.get(
    'VID_EXCL_THUMBNAIL_PROCESSING_TOPIC'
)

# The schema of the JSON in the event payload.
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
BQ_TARGET_TABLE_NAME = 'YouTubeThumbnailsWithAnnotations'


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
  logger.info('Thumbnail dispatcher process triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=message_schema)

  run(date_partition=message_json.get('date_partition'))

  logger.info('Done dispatching video_ids for thumbnail processing.')


def run(date_partition: str) -> None:
  """Gets new video ids from BigQuery table and triggers thumbnail processing.

  Args:
      date_partition: The date of the partition containing a list of video IDs
      to process.
  """
  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client()
  query = f'''
      SELECT DISTINCT video_id FROM {GOOGLE_CLOUD_PROJECT}.{BQ_DATASET}.{BQ_SOURCE_TABLE_NAME}
      WHERE TIMESTAMP_TRUNC(datetime_updated, DAY) = TIMESTAMP("{date_partition}")
      AND video_id NOT IN
        (SELECT DISTINCT video_id FROM {GOOGLE_CLOUD_PROJECT}.{BQ_DATASET}.{BQ_TARGET_TABLE_NAME})
  '''

  data = client.query(query).to_dataframe()
  if data.empty:
    logger.info('No new video IDs to process.')
  else:
    video_ids = data['video_id'].tolist()
    logger.info(
        '%d new video_id(s) to be dispatched for thumbnail processing.',
        len(video_ids),
    )
    _publish_videos_as_batch(
        project_id=GOOGLE_CLOUD_PROJECT,
        topic_id=THUMBNAIL_PROCESSING_TOPIC,
        video_ids=video_ids,
    )


def _publish_videos_as_batch(
    project_id: str,
    topic_id: str,
    video_ids: list[str],
) -> None:
  """Publishes multiple messages to a Pub/Sub topic with batch settings.

  Args:
      project_id: The Google Cloud Project with the pub/sub topic in.
      topic_id: The name of the topic to publish the message to.
      video_ids: The video IDs to publish to the topic.
  """

  batch_settings = pubsub_v1.types.BatchSettings(
      max_messages=100
  )
  publisher = pubsub_v1.PublisherClient(batch_settings)
  topic_path = publisher.topic_path(project_id, topic_id)
  publish_futures = []

  # Resolve the publish future in a separate thread.
  def callback(
      data: str,
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
          str(future.exception())
      )
    else:
      logger.info(
          'Message %s (%d/%d) published to %s.',
          data.decode('UTF-8'),
          current,
          total,
          topic_path
      )

  for current, video_id in enumerate(video_ids):
    message_str = json.dumps({'video_id': video_id})
    data = message_str.encode('utf-8')
    publish_future = publisher.publish(topic_path, data)
    publish_future.add_done_callback(
        functools.partial(callback, data, current+1, len(video_ids), topic_path)
    )
    publish_futures.append(publish_future)

  futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

  logger.info(
      'Published %d messages as a batch to %s.', len(video_ids), topic_path
  )
