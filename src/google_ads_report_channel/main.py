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
import datetime
import json
import logging
import os
import sys
from typing import Any, Dict, Optional, Tuple

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
from google.cloud import pubsub_v1
import jsonschema
import pandas as pd


logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project containing the GCS bucket
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The pub/sub topic to send the success message to
YOUTUBE_CHANNEL_PUBSUB_TOPIC = os.environ.get(
    'VID_EXCL_YOUTUBE_CHANNEL_PUBSUB_TOPIC'
)

# The schema of the JSON in the event payload
message_schema = {
    'type': 'object',
    'properties': {
        'customer_id': {'type': 'string'},
        'lookback_days': {'type': 'number'},
        'gads_filters': {'type': 'string'},
    },
    'required': [
        'customer_id',
        'lookback_days',
        'gads_filters',
    ],
}
# BQ Table name to store the Google Ads channel placement report. This is not
# expected to be configurable and so is not exposed as an environmental variable
BIGQUERY_TABLE_NAME = 'GoogleAdsReportChannel'


def main(event: Dict[str, Any], context: Dict[str, Any]) -> None:
  """The entry point: extract the data from the payload and starts the job.

  The pub/sub message must match the message_schema object above.

  Args:
      event: A dictionary representing the event data payload.
      context: An object containing metadata about the event.
  """
  del context
  logger.info('Google Ads Reporting Channels Service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=message_schema)

  run(
      message_json.get('customer_id'),
      message_json.get('lookback_days'),
      message_json.get('gads_filters'),
  )

  logger.info('Done')


def run(
    customer_id: str,
    lookback_days: int,
    gads_filters: str,
) -> None:
  """Starts the job to run the report from Google Ads & output it.

  Args:
      customer_id: The customer ID to fetch the Google Ads data for.
      lookback_days: The number of days from today to look back when fetching
        the report.
      gads_filters: The filters to apply to the Google Ads report query.
  """
  logger.info(
      'Starting job to process the channel placement report for customer'
      ' for %s',
      customer_id,
  )
  video_data = _get_video_placement_report(
      customer_id, lookback_days, gads_filters
  )

  if video_data.empty:
    logger.info('Not triggering YT jobs as there are no new videos.')
    return
  else:
    logger.info('Got %d records.', len(video_data.index))

  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  bq_client = bigquery.Client()
  timestamp = pd.Timestamp.now()

  query = f"""
      SELECT DISTINCT channel_id FROM {GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}
      WHERE TIMESTAMP_TRUNC(datetime_updated, DAY) = TIMESTAMP("{timestamp.date()}")
  """
  existing_channel_ids = bq_client.query(query).to_dataframe()

  if not existing_channel_ids.empty:
    video_data = video_data[
        ~video_data['channel_id'].isin(existing_channel_ids['channel_id'])
    ]

  if video_data.empty:
    logger.info('No new records after filtering.')
    return
  else:
    logger.info('%d new records remain after filtering.', len(video_data.index))

  video_data['datetime_updated'] = timestamp.floor('S')

  _write_results_to_bq(
      client=bq_client, data=video_data, table_name=BIGQUERY_TABLE_NAME
  )

  _send_message_to_pubsub(customer_id, str(timestamp.date()))

  logger.info(
      'Processing channel placement report for customer %s complete.',
      customer_id,
  )


def _get_video_placement_report(
    customer_id: str, lookback_days: int, gads_filters: str
) -> pd.DataFrame:
  """Runs the group placement report in Google Ads & return a Dataframe of the data.

  Args:
      customer_id: The customer ID to fetch the Google Ads data for.
      lookback_days: The number of days from today to look back when fetching
        the report.
      gads_filters: The filters to apply to the Google Ads report query.

  Returns:
      A Pandas DataFrame containing the report results.
  """
  logger.info('Getting report stream for %s', customer_id)
  client = GoogleAdsClient.load_from_env(version='v14')
  ga_service = client.get_service('GoogleAdsService')

  query = get_report_query(lookback_days, gads_filters)
  search_request = client.get_type('SearchGoogleAdsStreamRequest')
  search_request.customer_id = customer_id
  search_request.query = query
  stream = ga_service.search_stream(search_request)

  # The client and iterator needs to be in the same function, as per
  # https://github.com/googleads/google-ads-python/issues/384#issuecomment-791639397
  # So this can't be refactored out
  logger.info('Processing response stream')
  data = []
  for batch in stream:
    for row in batch.results:
      data.append([
          row.customer.id,
          row.group_placement_view.placement,
          row.group_placement_view.target_url,
          row.metrics.impressions,
          row.metrics.cost_micros,
          row.metrics.conversions,
          row.metrics.video_view_rate,
          row.metrics.video_views,
          row.metrics.clicks,
          row.metrics.average_cpm,
          row.metrics.ctr,
          row.metrics.all_conversions_from_interactions_rate,
      ])
    data = pd.DataFrame(
        data, columns=[
            'customer_id',
            'channel_id',
            'placement_target_url',
            'impressions',
            'cost_micros',
            'conversions',
            'video_view_rate',
            'video_views',
            'clicks',
            'average_cpm',
            'ctr',
            'all_conversions_from_interactions_rate',
        ],
    )

  data['customer_id'] = data['customer_id'].astype('string')

  return data


def get_report_query(
    lookback_days: int, gads_filters: Optional[str] = None
) -> str:
  """Builds and returns the Google Ads report query.

  Args:
      lookback_days: The number of days from today to look back when fetching
        the report.
      gads_filters: The filters to apply to the Google Ads report query.

  Returns:
      The Google Ads query.
  """
  logger.info('Getting chnanel report query')
  date_from, date_to = get_query_dates(lookback_days)
  if lookback_days > 1:
    where_query = f'AND segments.date BETWEEN "{date_from}" AND "{date_to}"'
  else:
    where_query = f'AND segments.date = "{date_to}"'
  if gads_filters is not None:
    where_query += f' AND {gads_filters}'
  query = f"""
        SELECT
            customer.id,
            group_placement_view.placement,
            group_placement_view.target_url,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.video_views,
            metrics.video_view_rate,
            metrics.clicks,
            metrics.average_cpm,
            metrics.ctr,
            metrics.all_conversions_from_interactions_rate
        FROM
            group_placement_view
        WHERE group_placement_view.placement_type = "YOUTUBE_CHANNEL"
            AND campaign.advertising_channel_type = "VIDEO"
            AND group_placement_view.display_name != ""
            AND group_placement_view.target_url != ""
            {where_query}
    """
  logger.info(query)
  return query


def get_query_dates(
    lookback_days: int, today: datetime = None
) -> Tuple[str, str]:
  """Returns a tuple of string dates in %Y-%m-%d format for the GAds report.

  Google Ads queries require a string date in the above format. This function
  will lookback X days from today, and return this date as a string.

  Args:
      lookback_days: The number of days from today to look back when fetching
        the report.
      today: The date representing today. If no date is provided
        datetime.today() is used.

  Returns:
      The string date.
  """
  logger.info('Getting query dates')
  dt_format = '%Y-%m-%d'
  if today is None:
    today = datetime.datetime.today()
  date_from = today - datetime.timedelta(days=lookback_days)
  return (
      date_from.strftime(dt_format),
      today.strftime(dt_format),
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
      write_disposition='WRITE_APPEND',
  )

  job = client.load_table_from_dataframe(
      dataframe=data, destination=destination, job_config=job_config
  )
  job.result()

  logger.info('Wrote %d records to table %s.', len(data.index), destination)


def _send_message_to_pubsub(customer_id: str, date_partition: str) -> None:
  """Pushes the customer ID to pub/sub when the job completes.

  Args:
      customer_id: The customer ID to fetch the Google Ads data for.
      date_partition: The partition of the BQ table.
  """
  message = json.dumps({
      'customer_id': customer_id,
      'date_partition': date_partition,
  })
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(
      GOOGLE_CLOUD_PROJECT, YOUTUBE_CHANNEL_PUBSUB_TOPIC
  )
  data = message.encode('utf-8')
  publisher.publish(topic_path, data)
  logger.info('Message published')
