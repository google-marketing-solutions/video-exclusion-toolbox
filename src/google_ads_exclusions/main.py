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
"""Output the placement report from Google Ads to BigQuery."""
import base64
import datetime
import json
import logging
import os
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict
import jsonschema
import pandas as pd
from utils import gcs

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The bucket to write the data to
VID_EXCL_GCS_DATA_BUCKET = os.environ.get('VID_EXCL_GCS_DATA_BUCKET')

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


def main(event: str, context: str) -> None:
  """The entry point: extract the data from the payload and starts the job.

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

  logger.info('Done')


def run(customer_id: str) -> None:
  """Start the job to run the report from Google Ads & output it.

  Args:
    customer_id: the customer ID to fetch the Google Ads data for.
  """
  logger.info('Starting job to fetch Google Ads exclusion data for %s',
              customer_id)
  exclusions_filename = f'google_ads_exclusions/{customer_id}.csv'
  df = get_exclusions(customer_id)
  write_results_to_gcs(exclusions_filename, df)
  logger.info('Job complete')


def get_exclusions(customer_id: str) -> pd.DataFrame:
  """Runs the group placement report in Google Ads & returns a Dataframe of the data.

  Args:
    customer_id: The customer ID to fetch the Google Ads data for.

  Returns:
    A Pandas DataFrame containing the report results.
  """
  logger.info('Getting report stream for %s', customer_id)
  client = GoogleAdsClient.load_from_env(version='v13')
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
    dictobj = MessageToDict(batch)
    shared_criterions.append(
        pd.json_normalize(dictobj, record_path=['results'])
    )

  df = pd.concat(shared_criterions, ignore_index=True)
  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

  df['customer_id'] = int(customer_id)
  df['datetime_updated'] = now

  df.loc[df['sharedCriterion.type'] == 'YOUTUBE_CHANNEL', 'id'] = df[
      'sharedCriterion.youtubeChannel.channelId'
  ]

  df.loc[df['sharedCriterion.type'] == 'YOUTUBE_VIDEO', 'id'] = df[
      'sharedCriterion.youtubeVideo.videoId'
  ]

  df.rename(
      columns={
          'sharedSet.name': 'exclusion_list',
          'sharedCriterion.type': 'exclusion_type',
      },
      inplace=True,
  )

  return df[[
      'id',
      'exclusion_list',
      'exclusion_type',
      'customer_id',
      'datetime_updated',
  ]]


def write_results_to_gcs(filename: str, df: pd.DataFrame) -> None:
  """Writes the report dataframe to GCS as a CSV file.

  Args:
    filename: Name of the file to write to GCS.
    df: The dataframe based on the Google Ads report.
  """
  number_of_rows = len(df.index)

  if number_of_rows > 0:
    logger.info(
        'Writing %d rows to GCS: {VID_EXCL_GCS_DATA_BUCKET}/%s.',
        number_of_rows,
        filename,
    )
    gcs.upload_blob_from_df(
        df=df, blob_name=filename, bucket=VID_EXCL_GCS_DATA_BUCKET
    )
    logger.info('Blob uploaded to GCS')
  else:
    logger.info('There is nothing to write to GCS')
