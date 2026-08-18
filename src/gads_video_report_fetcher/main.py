# Copyright 2026 Google LLC
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

"""Fetch Google Ads video placement reports and persist new records to BigQuery."""

import datetime
import io
import json
import os
import time
from typing import Any, Optional

import functions_framework
from google.ads.googleads import client as googleads_client
from google.ads.googleads import errors as gads_errors
import google.auth
from google.cloud import bigquery
from vet_common.bq import get_existing_partition_ids
from vet_common.bq import upsert_ndjson_to_bq
from vet_common.dates import get_lookback_date_range
from vet_common.events import parse_pubsub_cloudevent
from vet_common.gads import get_google_ads_client_version
from vet_common.ids import sanitize_gads_id
from vet_common.logging import get_service_logger
from vet_common.pubsub import publish_batch

logger = get_service_logger()

# Environment variables
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
YOUTUBE_VIDEO_PUBSUB_TOPIC = os.environ.get(
    'VID_EXCL_YOUTUBE_VIDEO_PUBSUB_TOPIC'
)
GOOGLE_ADS_CLIENT_VERSION = get_google_ads_client_version()
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
GOOGLE_ADS_USE_PROTO_PLUS = os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS', 'false')
BIGQUERY_TABLE_NAME = 'google_ads_report_video'

SCOPES = ['https://www.googleapis.com/auth/adwords']

# Payload schema for incoming Pub/Sub CloudEvent
MESSAGE_SCHEMA = {
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


def get_report_query(
    lookback_days: int,
    gads_filters: Optional[str] = None,
    customer_id: Optional[str] = None,
) -> str:
  """Builds and returns the GAQL query for video placement reporting."""
  date_from, date_to = get_lookback_date_range(lookback_days)
  if lookback_days > 1:
    where_query = f'AND segments.date BETWEEN "{date_from}" AND "{date_to}"'
  else:
    where_query = f'AND segments.date = "{date_to}"'
  if gads_filters:
    where_query += f' AND {gads_filters}'

  query = f"""
        SELECT
            customer.id,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            detail_placement_view.display_name,
            detail_placement_view.group_placement_target_url,
            detail_placement_view.placement,
            detail_placement_view.placement_type,
            detail_placement_view.target_url,
            metrics.impressions,
            metrics.cost_micros,
            metrics.conversions,
            metrics.video_trueview_views,
            metrics.video_trueview_view_rate,
            metrics.clicks,
            metrics.average_cpm,
            metrics.ctr,
            metrics.all_conversions_from_interactions_rate,
            metrics.video_quartile_p25_rate,
            metrics.video_quartile_p50_rate,
            metrics.video_quartile_p75_rate,
            metrics.video_quartile_p100_rate
        FROM
            detail_placement_view
        WHERE detail_placement_view.placement_type = "YOUTUBE_VIDEO"
            AND campaign.advertising_channel_type = "VIDEO"
            AND detail_placement_view.display_name != ""
            {where_query}
    """
  log_prefix = f'[{customer_id}] ' if customer_id else ''
  logger.info('%sGAQL Query: %s', log_prefix, query.strip().replace('\n', ' '))
  return query


def _execute_search_stream_with_retry(
    ga_service: Any,
    search_request: Any,
    customer_id: str = '',
    max_retries: int = 3,
    base_backoff_seconds: float = 2.0,
) -> Any:
  """Executes search_stream with exponential backoff on transient errors."""
  log_prefix = f'[{customer_id}] ' if customer_id else ''
  for attempt in range(1, max_retries + 1):
    try:
      return ga_service.search_stream(search_request)
    except gads_errors.GoogleAdsException as ex:
      error_str = str(ex).lower()
      is_transient = any(
          term in error_str
          for term in [
              'resource_exhausted',
              'unavailable',
              'deadline_exceeded',
              'internal_error',
          ]
      )
      if is_transient and attempt < max_retries:
        sleep_time = base_backoff_seconds * (2 ** (attempt - 1))
        logger.warning(
            '%sTransient Google Ads error on attempt %d/%d; retrying in'
            ' %.1fs: %s',
            log_prefix,
            attempt,
            max_retries,
            sleep_time,
            ex,
        )
        time.sleep(sleep_time)
      else:
        raise


def _fetch_and_stream_report_records(
    customer_id: str,
    lookback_days: int,
    gads_filters: str,
    max_retries: int = 3,
    base_backoff_seconds: float = 2.0,
) -> tuple[io.BytesIO, set[str], int]:
  """Queries Google Ads and streams records directly into an in-memory NDJSON buffer.

  Handles transient gRPC stream evaluation errors with exponential backoff and
  accurately recalculates derived rates across multi-day lookback date segments.

  Args:
      customer_id: The Google Ads customer ID.
      lookback_days: Number of days to look back in the reporting query.
      gads_filters: Optional additional GAQL filter clause.
      max_retries: Maximum attempts for transient network/API errors.
      base_backoff_seconds: Initial backoff duration in seconds.

  Returns:
      A tuple of (ndjson_buffer, retrieved_video_ids_set, total_row_count).
  """
  clean_customer_id = sanitize_gads_id(customer_id)
  logger.info('[%s] Querying Google Ads report stream...', clean_customer_id)
  credentials, _ = google.auth.default(scopes=SCOPES)
  login_customer_id = sanitize_gads_id(GOOGLE_ADS_LOGIN_CUSTOMER_ID)

  dev_token = (
      GOOGLE_ADS_DEVELOPER_TOKEN.strip() if GOOGLE_ADS_DEVELOPER_TOKEN else None
  )
  client_kwargs = {
      'credentials': credentials,
      'developer_token': dev_token,
      'version': GOOGLE_ADS_CLIENT_VERSION,
      'use_proto_plus': GOOGLE_ADS_USE_PROTO_PLUS.lower() == 'true',
  }
  if login_customer_id:
    client_kwargs['login_customer_id'] = login_customer_id

  client = googleads_client.GoogleAdsClient(**client_kwargs)
  ga_service = client.get_service('GoogleAdsService')

  query = get_report_query(
      lookback_days=lookback_days,
      gads_filters=gads_filters,
      customer_id=clean_customer_id,
  )
  search_request = client.get_type('SearchGoogleAdsStreamRequest')
  search_request.customer_id = clean_customer_id
  search_request.query = query

  for attempt in range(1, max_retries + 1):
    try:
      logger.info(
          '[%s] Processing response stream from Google Ads (attempt %d/%d)...',
          clean_customer_id,
          attempt,
          max_retries,
      )
      records_by_key: dict[tuple[str, int, int, str], dict[str, Any]] = {}
      now_iso = (
          datetime.datetime.now(datetime.timezone.utc)
          .replace(microsecond=0)
          .isoformat()
      )

      stream = ga_service.search_stream(search_request)
      for batch in stream:
        for row in batch.results:
          dp = row.detail_placement_view
          m = row.metrics
          video_id = dp.placement.strip() if dp.placement else ''
          if not video_id:
            continue

          campaign_id = int(row.campaign.id) if row.campaign.id else 0
          campaign_name = str(row.campaign.name) if row.campaign.name else ''
          ad_group_id = int(row.ad_group.id) if row.ad_group.id else 0
          ad_group_name = str(row.ad_group.name) if row.ad_group.name else ''

          placement_type_str = (
              dp.placement_type.name
              if hasattr(dp.placement_type, 'name')
              else str(dp.placement_type)
          )

          composite_key = (
              str(row.customer.id),
              campaign_id,
              ad_group_id,
              video_id,
          )

          if composite_key in records_by_key:
            existing = records_by_key[composite_key]
            existing['impressions'] += int(m.impressions)
            existing['cost_micros'] += int(m.cost_micros)
            existing['conversions'] += float(m.conversions)
            existing['video_views'] += int(m.video_trueview_views)
            existing['clicks'] += int(m.clicks)
            if dp.display_name and not existing['youtube_video_name']:
              existing['youtube_video_name'] = str(dp.display_name)
            if dp.target_url and not existing['youtube_video_url']:
              existing['youtube_video_url'] = str(dp.target_url)
            if (
                dp.group_placement_target_url
                and not existing['youtube_channel_url']
            ):
              existing['youtube_channel_url'] = str(
                  dp.group_placement_target_url
              )
          else:
            records_by_key[composite_key] = {
                'datetime_updated': now_iso,
                'customer_id': str(row.customer.id),
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'ad_group_id': ad_group_id,
                'ad_group_name': ad_group_name,
                'video_id': video_id,
                'youtube_video_name': str(dp.display_name or ''),
                'youtube_video_url': str(dp.target_url or ''),
                'placement_type': placement_type_str,
                'youtube_channel_url': str(dp.group_placement_target_url or ''),
                'impressions': int(m.impressions),
                'cost_micros': int(m.cost_micros),
                'conversions': float(m.conversions),
                'video_view_rate': float(m.video_trueview_view_rate or 0.0),
                'video_views': int(m.video_trueview_views),
                'clicks': int(m.clicks),
                'average_cpm': float(m.average_cpm or 0.0),
                'ctr': float(m.ctr or 0.0),
                'all_conversions_from_interactions_rate': float(
                    m.all_conversions_from_interactions_rate or 0.0
                ),
                'video_quartile_p25_rate': float(
                    m.video_quartile_p25_rate or 0.0
                ),
                'video_quartile_p50_rate': float(
                    m.video_quartile_p50_rate or 0.0
                ),
                'video_quartile_p75_rate': float(
                    m.video_quartile_p75_rate or 0.0
                ),
                'video_quartile_p100_rate': float(
                    m.video_quartile_p100_rate or 0.0
                ),
            }

      # Recalculate derived rates across aggregated multi-day date segments
      for record in records_by_key.values():
        impr = record['impressions']
        clicks = record['clicks']
        cost_micros = record['cost_micros']
        views = record['video_views']

        if impr > 0:
          record['ctr'] = float(clicks) / float(impr)
          record['average_cpm'] = (float(cost_micros) / float(impr)) * 1000.0
          record['video_view_rate'] = float(views) / float(impr)
        else:
          record['ctr'] = 0.0
          record['average_cpm'] = 0.0
          record['video_view_rate'] = 0.0

      buffer = io.BytesIO()
      for record in records_by_key.values():
        buffer.write(json.dumps(record).encode('utf-8') + b'\n')

      buffer.seek(0)
      retrieved_video_ids = {k[3] for k in records_by_key.keys()}
      row_count = len(records_by_key)
      return buffer, retrieved_video_ids, row_count

    except gads_errors.GoogleAdsException as ex:
      error_str = str(ex)
      if (
          'CUSTOMER_NOT_ENABLED' in error_str
          or 'CUSTOMER_NOT_FOUND' in error_str
      ):
        logger.warning(
            '[%s] Google Ads customer account is not enabled or deactivated'
            ' (CUSTOMER_NOT_ENABLED); skipping reporting pipeline.',
            clean_customer_id,
        )
        return io.BytesIO(), set(), 0

      is_transient = any(
          term in error_str.lower()
          for term in [
              'resource_exhausted',
              'unavailable',
              'deadline_exceeded',
              'internal_error',
          ]
      )
      if is_transient and attempt < max_retries:
        sleep_time = base_backoff_seconds * (2 ** (attempt - 1))
        logger.warning(
            '[%s] Transient Google Ads error on stream attempt %d/%d; retrying'
            ' in %.1fs: %s',
            clean_customer_id,
            attempt,
            max_retries,
            sleep_time,
            ex,
        )
        time.sleep(sleep_time)
      else:
        logger.error(
            '[%s] Failed to fetch Google Ads report: %s', clean_customer_id, ex
        )
        raise
    except Exception as ex:
      logger.error(
          '[%s] Unexpected error querying Google Ads report: %s',
          clean_customer_id,
          ex,
      )
      raise

  return io.BytesIO(), set(), 0


def run(
    customer_id: str,
    lookback_days: int,
    gads_filters: str,
) -> None:
  """Fetches video placements, upserts today's partition via MERGE, and notifies downstream."""
  logger.info(
      '[%s] Starting video placement report pipeline (lookback: %d days).',
      customer_id,
      lookback_days,
  )
  buffer, retrieved_video_ids, row_count = _fetch_and_stream_report_records(
      customer_id=customer_id,
      lookback_days=lookback_days,
      gads_filters=gads_filters,
  )

  if row_count == 0:
    logger.info(
        '[%s] No video placements returned from Google Ads.', customer_id
    )
    return

  logger.info(
      '[%s] Retrieved %d placements (%d unique video IDs) from Google Ads.',
      customer_id,
      row_count,
      len(retrieved_video_ids),
  )

  bq_client = bigquery.Client()
  today_date = datetime.date.today()

  existing_ids = get_existing_partition_ids(
      client=bq_client,
      project_id=GOOGLE_CLOUD_PROJECT,
      dataset_id=BIGQUERY_DATASET,
      table_name=BIGQUERY_TABLE_NAME,
      id_column='video_id',
      partition_date=today_date,
      logger=logger,
  )

  new_video_ids = retrieved_video_ids - existing_ids
  logger.info(
      "[%s] Discovered %d net-new video placements for today's partition (%s).",
      customer_id,
      len(new_video_ids),
      today_date,
  )

  upsert_ndjson_to_bq(
      client=bq_client,
      ndjson_buffer=buffer,
      project_id=GOOGLE_CLOUD_PROJECT,
      dataset_id=BIGQUERY_DATASET,
      table_name=BIGQUERY_TABLE_NAME,
      key_columns=['customer_id', 'campaign_id', 'ad_group_id', 'video_id'],
      partition_date=today_date,
      log_prefix=f'[{customer_id}] ',
      logger=logger,
  )

  if new_video_ids:
    publish_batch(
        project_id=GOOGLE_CLOUD_PROJECT,
        topic_id=YOUTUBE_VIDEO_PUBSUB_TOPIC,
        messages=[
            {'customer_id': customer_id, 'date_partition': str(today_date)}
        ],
        logger=logger,
    )
    logger.info(
        '[%s] Published delta notification to %s for %d new videos.',
        customer_id,
        YOUTUBE_VIDEO_PUBSUB_TOPIC,
        len(new_video_ids),
    )
  else:
    logger.info(
        '[%s] All %d placements updated in BigQuery via MERGE; no new videos'
        ' require downstream YouTube metadata retrieval.',
        customer_id,
        row_count,
    )

  logger.info('[%s] Completed video placement report pipeline.', customer_id)


@functions_framework.cloud_event
def main(cloud_event: functions_framework.cloud_event) -> None:
  """CloudEvent entrypoint for Google Ads video placement reporting."""
  payload = parse_pubsub_cloudevent(
      cloud_event=cloud_event,
      schema=MESSAGE_SCHEMA,
      logger=logger,
  )
  if not payload:
    return

  customer_id = sanitize_gads_id(str(payload['customer_id']))
  lookback_days = int(payload['lookback_days'])
  gads_filters = payload.get('gads_filters', '')

  try:
    run(
        customer_id=customer_id,
        lookback_days=lookback_days,
        gads_filters=gads_filters,
    )
  except Exception as e:  # pylint: disable=broad-except
    logger.error(
        '[%s] Error executing video placement reporting pipeline: %s',
        customer_id,
        e,
        exc_info=True,
    )
