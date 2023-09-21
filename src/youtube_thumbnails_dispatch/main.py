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

Retrieve a file from GCS containing Youtube Video IDs and dispatch each
ID to a Pub/Sub topic for further processing.
"""
import base64
import datetime
import hashlib
import json
import logging
import os
import sys
import time
from typing import Any, Dict

from google.api_core import exceptions
import google.auth
import google.auth.credentials
from google.cloud import bigquery
from google.cloud import pubsub_v1
import jsonschema
import pandas as pd

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The bucket to read the file with video IDs from.
GCS_DATA_BUCKET = os.environ.get('VID_EXCL_GCS_DATA_BUCKET')
# The bucket to read the file with video IDs from.
THUMBNAIL_PROCESSING_TOPIC = os.environ.get(
    'VID_EXCL_THUMBNAIL_PROCESSING_TOPIC'
)

# The schema of the JSON in the event payload.
message_schema = {
    'type': 'object',
    'properties': {
        'customer_id': {'type': 'string'},
        'blob_name': {'type': 'string'},
    },
    'required': [
        'blob_name',
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
  logger.info('Thumbnail dispatcher process triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=message_schema)

  run(blob_name=message_json.get('blob_name'))

  logger.info('Done dispatching video_ids for thumbnail processing.')


def run(blob_name: str) -> None:
  """Orchestration to process all videos in a csv file.

  Args:
      blob_name: The name of the file containing a list of video IDs to process.
  """

  credentials = get_auth_credentials()

  video_data = pd.read_csv(f'gs://{GCS_DATA_BUCKET}/{blob_name}')
  video_ids = video_data[['video_id']]
  logger.info('Checking new videos.')
  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client(
      project=GOOGLE_CLOUD_PROJECT, credentials=credentials
  )
  temp_table = temp_table_from_csv(video_ids, client)
  query = f"""
            SELECT video_id
            FROM
            `{BQ_DATASET}.{temp_table}`
            WHERE
            video_id NOT IN
              (SELECT
                video_id
              FROM `{BQ_DATASET}.YouTubeThumbnailsWithAnnotations`)
            """
  rows = client.query(query).result()
  if rows.total_rows is not None and rows.total_rows > 0:
    logger.info(
        '%d new video_id(s) to be dispatched for thumbnail processing.',
        rows.total_rows,
    )
    counter = 1
    for row in rows:
      _send_video_for_processing(
          video_id=row.video_id,
          topic=THUMBNAIL_PROCESSING_TOPIC,
          gcp_project=GOOGLE_CLOUD_PROJECT,
      )
      logger.info('Dispatched %d of %d.', counter, rows.total_rows)
      counter += 1
  else:
    logger.info('No new video IDs to process.')


def _send_video_for_processing(
    video_id: str, topic: str, gcp_project: str
) -> None:
  """Pushes the dictionary to pubsub.

  Args:
      video_id: The viseo ID to push to pubsub.
      topic: The name of the topic to publish the message to.
      gcp_project: The Google Cloud Project with the pub/sub topic in.
  """
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(gcp_project, topic)
  message_str = json.dumps({'video_id': video_id})
  # Data must be a bytestring
  data = message_str.encode('utf-8')
  publisher.publish(topic_path, data)
  logger.info('Video %s dispatched for thumbnail processing.', video_id)


def temp_table_from_csv(data: pd.DataFrame, client: bigquery.Client) -> str:
  """Creates a temporary BQ table to store video IDs for querying.

  Args:
      data: A dataframe containing the video IDs to be written.
      client: A BigQuery client object.

  Returns:
      The name of the temporary table.
  """
  timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
  suffix = hashlib.sha256(timestamp.encode('utf-8')).hexdigest()[:6]
  table_name = '-'.join(['temp-video-ids', suffix, timestamp])
  destination = '.'.join([GOOGLE_CLOUD_PROJECT, BQ_DATASET, table_name])
  logger.info('Creating a temporary table: %s', table_name)

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField(
              'video_id', bigquery.enums.SqlTypeNames.STRING
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

  logger.info('Table \'%s\' created.', table_name)

  return table_name


def get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials
