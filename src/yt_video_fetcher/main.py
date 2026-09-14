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
"""Pulls YouTube video metadata for placements discovered in Google Ads reports."""

import datetime
import io
import json
import os
from typing import Any, Optional, Sequence

import cloudevents.http
import functions_framework
import google.auth
from google.cloud import bigquery
from googleapiclient import discovery
from googleapiclient import errors as googleapiclient_errors
from vet_common.bq import upsert_ndjson_to_bq
from vet_common.events import parse_pubsub_cloudevent
from vet_common.logging import PipelineTelemetryContext, get_service_logger
from vet_common.pubsub import publish_batch

logger = get_service_logger()

# Configuration from environment variables
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET', '')
SOURCE_TABLE_NAME = os.environ.get(
    'VET_BIGQUERY_SOURCE_TABLE', 'google_ads_report_video'
)
TARGET_TABLE_NAME = os.environ.get('VET_BIGQUERY_TARGET_TABLE', 'youtube_video')
DOWNSTREAM_TOPIC = os.environ.get('VID_EXCL_THUMBNAILS_PUBSUB_TOPIC')

# Maximum number of videos per YouTube API request (YouTube API limit is 50)
CHUNK_SIZE = 50

# Schema definition for incoming Pub/Sub CloudEvent payload
MESSAGE_SCHEMA = {
    'type': 'object',
    'properties': {
        'date_partition': {'type': 'string'},
        'customer_id': {'type': 'string'},
        'run_id': {'type': 'string'},
    },
    'required': ['date_partition'],
}


def _build_youtube_service() -> Any:
  """Builds and returns an authorized YouTube Data API v3 service client."""
  credentials, _ = google.auth.default(
      scopes=['https://www.googleapis.com/auth/youtube.readonly']
  )
  return discovery.build(
      'youtube',
      'v3',
      credentials=credentials,
      cache_discovery=False,
  )


def get_unprocessed_video_ids(
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    source_table: str,
    target_table: str,
    date_partition: Optional[str] = None,
    customer_id: Optional[str] = None,
) -> list[str]:
  """Queries BigQuery for video IDs present in source placements missing in target metadata table.

  Supports flexible date_partition: if provided and not 'all', prunes the source
  table to the specific date partition. If omitted or 'all', scans across all
  historical source placements.
  """
  where_clauses = [
      'video_id IS NOT NULL',
      'TRIM(video_id) != ""',
  ]
  query_params: list[Any] = []

  if date_partition and date_partition.lower() != 'all':
    where_clauses.append('DATE(datetime_updated) = DATE(@date_partition)')
    query_params.append(
        bigquery.ScalarQueryParameter(
            'date_partition', 'STRING', date_partition
        )
    )

  if customer_id and str(customer_id).lower() != 'all':
    where_clauses.append('customer_id = @customer_id')
    query_params.append(
        bigquery.ScalarQueryParameter('customer_id', 'STRING', str(customer_id))
    )

  where_sql = ' AND '.join(where_clauses)

  query = f"""
    SELECT DISTINCT video_id
    FROM `{project_id}.{dataset_id}.{source_table}`
    WHERE {where_sql}
      AND video_id NOT IN (
        SELECT DISTINCT video_id
        FROM `{project_id}.{dataset_id}.{target_table}`
        WHERE video_id IS NOT NULL AND TRIM(video_id) != ""
      )
  """
  job_config = bigquery.QueryJobConfig(query_parameters=query_params)
  query_job = bq_client.query(query, job_config=job_config)
  results = query_job.result()
  return [str(row.video_id) for row in results if row.video_id]


def chunk_list(items: Sequence[Any], chunk_size: int) -> list[list[Any]]:
  """Splits a sequence into sublists of size up to chunk_size."""
  return [
      list(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)
  ]


def fetch_video_metadata_from_youtube(
    youtube_service: Any,
    video_ids: Sequence[str],
    now_iso: str,
) -> list[dict[str, Any]]:
  """Fetches snippet, contentDetails, and statistics for videos in chunks of 50."""
  records: list[dict[str, Any]] = []
  chunks = chunk_list(list(video_ids), CHUNK_SIZE)
  total_chunks = len(chunks)

  for idx, chunk in enumerate(chunks, start=1):
    logger.info(
        'Fetching YouTube video metadata chunk %d of %d (%d videos)...',
        idx,
        total_chunks,
        len(chunk),
    )
    try:
      request = youtube_service.videos().list(
          part='id,snippet,contentDetails,statistics',
          id=','.join(chunk),
          maxResults=CHUNK_SIZE,
      )
      response = request.execute(num_retries=3)
    except googleapiclient_errors.HttpError as err:
      logger.error('YouTube API HTTP error on chunk %d: %s', idx, err)
      raise

    items = response.get('items', [])
    found_video_ids = set()

    for item in items:
      video_id = item.get('id')
      if not video_id:
        continue
      found_video_ids.add(video_id)

      snippet = item.get('snippet') or {}
      content_details = item.get('contentDetails') or {}
      statistics = item.get('statistics') or {}
      content_rating = content_details.get('contentRating') or {}

      tags = [str(t) for t in snippet.get('tags') or [] if t]

      records.append({
          'video_id': str(video_id),
          'title': str(snippet.get('title') or ''),
          'description': str(snippet.get('description') or ''),
          'publishedAt': snippet.get('publishedAt') or None,
          'channelId': str(snippet.get('channelId') or ''),
          'categoryId': int(snippet.get('categoryId') or 0),
          'tags': tags,
          'defaultLanguage': str(
              snippet.get('defaultLanguage')
              or snippet.get('defaultAudioLanguage')
              or ''
          ),
          'duration': str(content_details.get('duration') or ''),
          'definition': str(content_details.get('definition') or ''),
          'licensedContent': bool(
              content_details.get('licensedContent', False)
          ),
          'ytContentRating': str(content_rating.get('ytRating') or ''),
          'viewCount': int(statistics.get('viewCount') or 0),
          'likeCount': int(statistics.get('likeCount') or 0),
          'commentCount': int(statistics.get('commentCount') or 0),
          'datetime_updated': now_iso,
          'availability_status': 'ACTIVE',
          'unavailable_reason': None,
          'last_checked_at': now_iso,
      })

    # Record tombstones for videos deleted, privated, or suspended on YouTube
    missing_ids = set(chunk) - found_video_ids
    if missing_ids:
      logger.warning(
          'YouTube API returned no results for %d video(s) (tombstone records'
          ' created): %s',
          len(missing_ids),
          list(missing_ids)[:10],
      )
      for missing_id in missing_ids:
        records.append({
            'video_id': str(missing_id),
            'title': '',
            'description': '',
            'publishedAt': None,
            'channelId': '',
            'categoryId': 0,
            'tags': [],
            'defaultLanguage': '',
            'duration': '',
            'definition': '',
            'licensedContent': False,
            'ytContentRating': '',
            'viewCount': 0,
            'likeCount': 0,
            'commentCount': 0,
            'datetime_updated': now_iso,
            'availability_status': 'DELETED',
            'unavailable_reason': 'NOT_FOUND_ON_YOUTUBE',
            'last_checked_at': now_iso,
        })

  return records


def run(
    date_partition: Optional[str] = None,
    customer_id: Optional[str] = None,
    run_id: Optional[str] = None,
    telemetry: Optional[PipelineTelemetryContext] = None,
) -> int:
  """Main execution logic to discover, fetch YouTube video metadata, and upsert to BigQuery."""
  telemetry = telemetry or PipelineTelemetryContext(
      logger=logger,
      service_name=os.environ.get('K_SERVICE', 'vet-yt-video-fetcher'),
      customer_id=customer_id or 'all',
      run_id=run_id,
  )

  logger.info(
      'Starting YouTube video fetcher for partition: %s (customer: %s)',
      date_partition or 'all',
      customer_id or 'all',
  )

  bq_client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)

  # Step 1: Identify videos needing metadata extraction
  unprocessed_video_ids = get_unprocessed_video_ids(
      bq_client=bq_client,
      project_id=GOOGLE_CLOUD_PROJECT,
      dataset_id=BIGQUERY_DATASET,
      source_table=SOURCE_TABLE_NAME,
      target_table=TARGET_TABLE_NAME,
      date_partition=date_partition,
      customer_id=customer_id,
  )

  telemetry.log_step(
      step='DISCOVER_UNPROCESSED_VIDEOS',
      records_out=len(unprocessed_video_ids),
      metadata={
          'date_partition': date_partition or 'all',
          'customer_id': customer_id or 'all',
      },
  )

  if not unprocessed_video_ids:
    logger.info(
        'No new unprocessed videos found for partition %s.',
        date_partition or 'all',
    )
    telemetry.log_step(
        step='SKIPPED',
        records_out=0,
        metadata={'reason': 'No new video IDs found in source table'},
    )
    return 0

  logger.info(
      'Discovered %d new video(s) to fetch from YouTube API.',
      len(unprocessed_video_ids),
  )

  # Step 2: Fetch metadata from YouTube API
  now_iso = (
      datetime.datetime.now(datetime.timezone.utc)
      .replace(microsecond=0)
      .isoformat()
  )
  youtube_service = _build_youtube_service()

  records = fetch_video_metadata_from_youtube(
      youtube_service=youtube_service,
      video_ids=unprocessed_video_ids,
      now_iso=now_iso,
  )

  telemetry.log_step(
      step='FETCH_YOUTUBE_METADATA',
      records_in=len(unprocessed_video_ids),
      records_out=len(records),
  )

  # Step 3: Serialize to NDJSON in-memory buffer
  buffer = io.BytesIO()
  for record in records:
    buffer.write(json.dumps(record).encode('utf-8'))
    buffer.write(b'\n')
  buffer.seek(0)

  # Step 4: BigQuery Upsert (idempotent DML MERGE across entire table on video_id)
  upsert_ndjson_to_bq(
      client=bq_client,
      ndjson_buffer=buffer,
      project_id=GOOGLE_CLOUD_PROJECT,
      dataset_id=BIGQUERY_DATASET,
      table_name=TARGET_TABLE_NAME,
      key_columns=['video_id'],
      partition_date=None,
      log_prefix=f'[{customer_id or "all"}] ',
      logger=logger,
  )

  telemetry.log_step(
      step='UPSERT_BIGQUERY',
      records_in=len(records),
      records_out=len(records),
  )

  # Step 5: Downstream notification (if configured)
  if DOWNSTREAM_TOPIC:
    publish_batch(
        project_id=GOOGLE_CLOUD_PROJECT,
        topic_name=DOWNSTREAM_TOPIC,
        messages=[{'video_ids': [r['video_id'] for r in records]}],
        logger=logger,
    )
    telemetry.log_step(
        step='PUBLISH_DOWNSTREAM',
        records_in=len(records),
        records_out=len(records),
    )

  logger.info(
      'Successfully fetched and saved %d YouTube video records.', len(records)
  )
  telemetry.log_step(
      step='COMPLETE',
      records_in=len(unprocessed_video_ids),
      records_out=len(records),
  )
  return len(records)


@functions_framework.cloud_event
def main(cloud_event: cloudevents.http.CloudEvent) -> tuple[str, int]:
  """Eventarc Cloud Function entry point triggered by Pub/Sub."""
  event_id = cloud_event.get('id', '')
  telemetry = PipelineTelemetryContext(
      logger=logger,
      service_name=os.environ.get('K_SERVICE', 'vet-yt-video-fetcher'),
      run_id=event_id,
  )

  try:
    data = parse_pubsub_cloudevent(cloud_event, schema=MESSAGE_SCHEMA)
  except Exception as e:  # pylint: disable=broad-exception-caught
    logger.exception('Failed to parse CloudEvent payload: %s', e)
    telemetry.log_step(
        step='ERROR',
        status='FAILED',
        error=e,
        metadata={'event_id': event_id},
    )
    return f'Bad Request: {e}', 400

  if not data:
    logger.warning('CloudEvent contained invalid or empty payload; skipping.')
    telemetry.log_step(
        step='ERROR',
        status='FAILED',
        metadata={
            'event_id': event_id,
            'error': 'Payload schema validation failed',
        },
    )
    return 'Invalid message schema', 400

  date_partition = data['date_partition']
  customer_id = data.get('customer_id')
  run_id = data.get('run_id') or event_id
  telemetry.set_customer_id(customer_id)

  try:
    run(
        date_partition=date_partition,
        customer_id=customer_id,
        run_id=run_id,
        telemetry=telemetry,
    )
    return 'OK', 200
  except Exception as e:  # pylint: disable=broad-exception-caught
    logger.exception(
        'Unexpected error executing YouTube video fetcher pipeline: %s', e
    )
    telemetry.log_step(
        step='ERROR',
        status='FAILED',
        error=e,
        metadata={
            'date_partition': date_partition,
            'customer_id': customer_id,
        },
    )
    return f'Internal Server Error: {e}', 500
