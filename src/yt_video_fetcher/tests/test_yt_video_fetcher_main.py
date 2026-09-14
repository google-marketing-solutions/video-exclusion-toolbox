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
"""Unit tests for the yt_video_fetcher module."""

import base64
import json
from typing import Any
from unittest import mock

from googleapiclient import errors as googleapiclient_errors
import httplib2
import pytest
from yt_video_fetcher import main


class MockCloudEvent:
  """Mock CloudEvent representation for unit tests."""

  def __init__(
      self,
      data: dict[str, Any],
      attributes: dict[str, Any] | None = None,
  ):
    self.data = data
    self._attributes = attributes or {}

  def get(self, key: str, default: Any = None) -> Any:
    return self._attributes.get(key, default)

  def __getitem__(self, key: str) -> Any:
    return self._attributes[key]


def test_chunk_list():
  """Tests chunking of lists."""
  items = list(range(125))
  chunks = main.chunk_list(items, 50)
  assert len(chunks) == 3
  assert len(chunks[0]) == 50
  assert len(chunks[1]) == 50
  assert len(chunks[2]) == 25


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_video_ids_with_partition_and_customer(
    mock_bq_client_cls,
):
  """Tests fetching unprocessed video IDs with partition and customer_id filters."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job

  row1 = mock.MagicMock()
  row1.video_id = 'vid_001'
  row2 = mock.MagicMock()
  row2.video_id = 'vid_002'
  mock_query_job.result.return_value = [row1, row2]

  video_ids = main.get_unprocessed_video_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_video',
      target_table='youtube_video',
      date_partition='2026-09-10',
      customer_id='1234567890',
  )
  assert video_ids == ['vid_001', 'vid_002']
  query_arg = mock_client.query.call_args[0][0]
  assert 'DATE(datetime_updated) = DATE(@date_partition)' in query_arg
  assert 'customer_id = @customer_id' in query_arg


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_video_ids_backfill_all_omits_partition_filter(
    mock_bq_client_cls,
):
  """Tests that date_partition='all' omits partition clause to scan all history."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job
  mock_query_job.result.return_value = []

  video_ids = main.get_unprocessed_video_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_video',
      target_table='youtube_video',
      date_partition='all',
      customer_id='1234567890',
  )
  assert video_ids == []
  query_arg = mock_client.query.call_args[0][0]
  assert '@date_partition' not in query_arg
  assert 'customer_id = @customer_id' in query_arg


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_video_ids_customer_all_omits_filter(
    mock_bq_client_cls,
):
  """Tests that customer_id='all' omits customer filter clause."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job
  mock_query_job.result.return_value = []

  video_ids = main.get_unprocessed_video_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_video',
      target_table='youtube_video',
      date_partition='2026-09-10',
      customer_id='all',
  )
  assert video_ids == []
  query_arg = mock_client.query.call_args[0][0]
  assert 'customer_id = @customer_id' not in query_arg


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_video_ids_none_partition_omits_filter(
    mock_bq_client_cls,
):
  """Tests that date_partition=None omits partition filter clause."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job
  mock_query_job.result.return_value = []

  video_ids = main.get_unprocessed_video_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_video',
      target_table='youtube_video',
      date_partition=None,
      customer_id=None,
  )
  assert video_ids == []
  query_arg = mock_client.query.call_args[0][0]
  assert '@date_partition' not in query_arg
  assert '@customer_id' not in query_arg


def test_fetch_video_metadata_from_youtube_valid_response():
  """Tests parsing and enriching videos from YouTube Data API response."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.videos().list.return_value = mock_request

  mock_response = {
      'items': [{
          'id': 'vid_sample_123',
          'snippet': {
              'title': 'Sample Video Title',
              'description': 'Sample Video Description',
              'publishedAt': '2026-09-10T08:00:00Z',
              'channelId': 'UC_channel_xyz',
              'categoryId': '24',
              'tags': ['google', 'marketing', 'tech'],
              'defaultLanguage': 'en',
          },
          'contentDetails': {
              'duration': 'PT10M15S',
              'definition': 'hd',
              'licensedContent': True,
              'contentRating': {
                  'ytRating': 'ytAgeRestricted',
              },
          },
          'statistics': {
              'viewCount': '50000',
              'likeCount': '1200',
              'commentCount': '85',
          },
      }]
  }
  mock_request.execute.return_value = mock_response

  records = main.fetch_video_metadata_from_youtube(
      youtube_service=mock_youtube,
      video_ids=['vid_sample_123'],
      now_iso='2026-09-10T12:00:00Z',
  )
  assert len(records) == 1
  rec = records[0]
  assert rec['video_id'] == 'vid_sample_123'
  assert rec['title'] == 'Sample Video Title'
  assert rec['description'] == 'Sample Video Description'
  assert rec['publishedAt'] == '2026-09-10T08:00:00Z'
  assert rec['channelId'] == 'UC_channel_xyz'
  assert rec['categoryId'] == 24
  assert rec['tags'] == ['google', 'marketing', 'tech']
  assert rec['defaultLanguage'] == 'en'
  assert rec['duration'] == 'PT10M15S'
  assert rec['definition'] == 'hd'
  assert rec['licensedContent'] is True
  assert rec['ytContentRating'] == 'ytAgeRestricted'
  assert rec['viewCount'] == 50000
  assert rec['likeCount'] == 1200
  assert rec['commentCount'] == 85
  assert rec['datetime_updated'] == '2026-09-10T12:00:00Z'
  assert rec['availability_status'] == 'ACTIVE'
  assert rec['unavailable_reason'] is None
  assert rec['last_checked_at'] == '2026-09-10T12:00:00Z'


def test_fetch_video_metadata_from_youtube_tombstone_handling():
  """Tests tombstone generation for deleted or private videos."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.videos().list.return_value = mock_request

  # YouTube API returns empty items list for non-existent/deleted video
  mock_request.execute.return_value = {'items': []}

  records = main.fetch_video_metadata_from_youtube(
      youtube_service=mock_youtube,
      video_ids=['vid_deleted_404'],
      now_iso='2026-09-10T12:00:00Z',
  )
  assert len(records) == 1
  rec = records[0]
  assert rec['video_id'] == 'vid_deleted_404'
  assert rec['title'] == ''
  assert rec['description'] == ''
  assert rec['publishedAt'] is None
  assert rec['channelId'] == ''
  assert rec['categoryId'] == 0
  assert rec['tags'] == []
  assert rec['viewCount'] == 0
  assert rec['likeCount'] == 0
  assert rec['commentCount'] == 0
  assert rec['licensedContent'] is False
  assert rec['datetime_updated'] == '2026-09-10T12:00:00Z'
  assert rec['availability_status'] == 'DELETED'
  assert rec['unavailable_reason'] == 'NOT_FOUND_ON_YOUTUBE'
  assert rec['last_checked_at'] == '2026-09-10T12:00:00Z'


def test_fetch_video_metadata_from_youtube_item_without_id_is_skipped():
  """Tests that a malformed API item lacking an 'id' is skipped, not crashed on.

  The item is discarded rather than partially written, so the requested video
  still falls through to tombstone handling and is not silently lost.
  """
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.videos().list.return_value = mock_request
  mock_request.execute.return_value = {
      'items': [
          {'snippet': {'title': 'Orphan item with no id'}},
          {
              'id': 'vid_ok',
              'snippet': {'title': 'Good video', 'publishedAt': '2026-01-01'},
          },
      ]
  }

  records = main.fetch_video_metadata_from_youtube(
      youtube_service=mock_youtube,
      video_ids=['vid_ok', 'vid_missing'],
      now_iso='2026-09-10T12:00:00Z',
  )

  by_id = {r['video_id']: r for r in records}
  # The orphan contributed no record of its own.
  assert set(by_id) == {'vid_ok', 'vid_missing'}
  assert by_id['vid_ok']['availability_status'] == 'ACTIVE'
  # vid_missing was requested but never returned, so it is tombstoned.
  assert by_id['vid_missing']['availability_status'] == 'DELETED'


@mock.patch.object(main.discovery, 'build')
@mock.patch.object(main.google.auth, 'default')
def test_build_youtube_service_requests_readonly_scope(
    mock_auth_default, mock_build
):
  """Tests the YouTube client is built with the read-only scope.

  A wrong scope or API version fails only at runtime against real credentials,
  so it is pinned here.
  """
  mock_credentials = mock.MagicMock()
  mock_auth_default.return_value = (mock_credentials, 'test-project')

  service = main._build_youtube_service()  # pylint: disable=protected-access

  mock_auth_default.assert_called_once_with(
      scopes=['https://www.googleapis.com/auth/youtube.readonly']
  )
  mock_build.assert_called_once_with(
      'youtube',
      'v3',
      credentials=mock_credentials,
      cache_discovery=False,
  )
  assert service is mock_build.return_value


def test_fetch_video_metadata_from_youtube_handles_null_statistics():
  """Tests graceful fallback when statistics or contentDetails are null."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.videos().list.return_value = mock_request

  mock_response = {
      'items': [{
          'id': 'vid_no_stats',
          'snippet': {
              'title': 'No Stats Video',
          },
          'contentDetails': None,
          'statistics': None,
      }]
  }
  mock_request.execute.return_value = mock_response

  records = main.fetch_video_metadata_from_youtube(
      youtube_service=mock_youtube,
      video_ids=['vid_no_stats'],
      now_iso='2026-09-10T12:00:00Z',
  )
  assert len(records) == 1
  rec = records[0]
  assert rec['video_id'] == 'vid_no_stats'
  assert rec['title'] == 'No Stats Video'
  assert rec['viewCount'] == 0
  assert rec['likeCount'] == 0
  assert rec['commentCount'] == 0
  assert rec['duration'] == ''
  assert rec['licensedContent'] is False


def test_fetch_video_metadata_from_youtube_raises_http_error():
  """Tests that YouTube API HTTP errors are logged and propagated."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.videos().list.return_value = mock_request

  resp = httplib2.Response({'status': 403, 'reason': 'Quota Exceeded'})
  mock_request.execute.side_effect = googleapiclient_errors.HttpError(
      resp, b'Quota Exceeded'
  )

  with pytest.raises(googleapiclient_errors.HttpError):
    main.fetch_video_metadata_from_youtube(
        youtube_service=mock_youtube,
        video_ids=['vid_error'],
        now_iso='2026-09-10T12:00:00Z',
    )


@mock.patch.object(main, 'get_unprocessed_video_ids')
@mock.patch.object(main.bigquery, 'Client')
def test_run_no_videos_to_process_returns_zero(
    mock_bq_client_cls, mock_get_unprocessed
):
  """Tests run exits early and returns 0 when no unprocessed videos exist."""
  mock_get_unprocessed.return_value = []
  count = main.run(date_partition='2026-09-10', customer_id='1234567890')
  assert count == 0


@mock.patch.object(main, 'upsert_ndjson_to_bq')
@mock.patch.object(main, 'fetch_video_metadata_from_youtube')
@mock.patch.object(main, '_build_youtube_service')
@mock.patch.object(main, 'get_unprocessed_video_ids')
@mock.patch.object(main.bigquery, 'Client')
def test_run_successful_fetch_and_upsert(
    mock_bq_client_cls,
    mock_get_unprocessed,
    mock_build_yt,
    mock_fetch_yt,
    mock_upsert_bq,
):
  """Tests end-to-end run execution with upsert."""
  mock_get_unprocessed.return_value = ['vid_1', 'vid_2']
  mock_fetch_yt.return_value = [
      {
          'video_id': 'vid_1',
          'title': 'Video 1',
          'description': 'Desc 1',
          'publishedAt': '2026-09-10T10:00:00Z',
          'channelId': 'UC_chan1',
          'categoryId': 10,
          'tags': ['music'],
          'defaultLanguage': 'en',
          'duration': 'PT3M',
          'definition': 'hd',
          'licensedContent': False,
          'ytContentRating': '',
          'viewCount': 100,
          'likeCount': 10,
          'commentCount': 2,
          'datetime_updated': '2026-09-10T12:00:00Z',
      },
      {
          'video_id': 'vid_2',
          'title': 'Video 2',
          'description': 'Desc 2',
          'publishedAt': '2026-09-10T11:00:00Z',
          'channelId': 'UC_chan2',
          'categoryId': 20,
          'tags': ['gaming'],
          'defaultLanguage': 'en',
          'duration': 'PT20M',
          'definition': 'hd',
          'licensedContent': True,
          'ytContentRating': '',
          'viewCount': 200,
          'likeCount': 20,
          'commentCount': 5,
          'datetime_updated': '2026-09-10T12:00:00Z',
      },
  ]

  count = main.run(
      date_partition='2026-09-10', customer_id='1234567890', run_id='test-run'
  )
  assert count == 2
  mock_upsert_bq.assert_called_once()
  assert mock_upsert_bq.call_args[1]['key_columns'] == ['video_id']
  assert mock_upsert_bq.call_args[1]['partition_date'] is None


@mock.patch.object(main, 'publish_batch', autospec=True)
@mock.patch.object(main, 'DOWNSTREAM_TOPIC', 'test-downstream-topic')
@mock.patch.object(main, 'upsert_ndjson_to_bq')
@mock.patch.object(main, 'fetch_video_metadata_from_youtube')
@mock.patch.object(main, '_build_youtube_service')
@mock.patch.object(main, 'get_unprocessed_video_ids')
@mock.patch.object(main.bigquery, 'Client')
def test_run_downstream_topic_configured_publishes_with_topic_id(
    mock_bq_client_cls,
    mock_get_unprocessed,
    mock_build_yt,
    mock_fetch_yt,
    mock_upsert_bq,
    mock_publish_batch,
):
  """Tests that the downstream notification uses publish_batch's real signature.

  The mock is autospecced, so passing an argument name that does not exist on
  vet_common.pubsub.publish_batch raises TypeError here rather than at runtime
  in production. This branch is dead in the current deployment because
  VID_EXCL_THUMBNAILS_PUBSUB_TOPIC is not set in Terraform, so it needs
  explicit coverage.
  """
  mock_get_unprocessed.return_value = ['vid_1']
  mock_fetch_yt.return_value = [{
      'video_id': 'vid_1',
      'title': 'Video 1',
      'datetime_updated': '2026-09-10T12:00:00Z',
  }]

  count = main.run(
      date_partition='2026-09-10', customer_id='1234567890', run_id='test-run'
  )

  assert count == 1
  mock_publish_batch.assert_called_once()
  kwargs = mock_publish_batch.call_args[1]
  assert kwargs['topic_id'] == 'test-downstream-topic'
  assert kwargs['messages'] == [{'video_ids': ['vid_1']}]


@mock.patch.object(main, 'publish_batch', autospec=True)
@mock.patch.object(main, 'DOWNSTREAM_TOPIC', None)
@mock.patch.object(main, 'upsert_ndjson_to_bq')
@mock.patch.object(main, 'fetch_video_metadata_from_youtube')
@mock.patch.object(main, '_build_youtube_service')
@mock.patch.object(main, 'get_unprocessed_video_ids')
@mock.patch.object(main.bigquery, 'Client')
def test_run_downstream_topic_unset_skips_publish(
    mock_bq_client_cls,
    mock_get_unprocessed,
    mock_build_yt,
    mock_fetch_yt,
    mock_upsert_bq,
    mock_publish_batch,
):
  """Tests that no downstream publish occurs when the topic is not configured."""
  mock_get_unprocessed.return_value = ['vid_1']
  mock_fetch_yt.return_value = [{
      'video_id': 'vid_1',
      'title': 'Video 1',
      'datetime_updated': '2026-09-10T12:00:00Z',
  }]

  count = main.run(
      date_partition='2026-09-10', customer_id='1234567890', run_id='test-run'
  )

  assert count == 1
  mock_publish_batch.assert_not_called()


@mock.patch.object(main, 'run')
def test_main_cloudevent_valid_payload_returns_200(mock_run):
  """Tests CloudEvent entrypoint with valid payload."""
  payload = {
      'message': {
          'data': (
              base64.b64encode(
                  json.dumps({
                      'date_partition': '2026-09-10',
                      'customer_id': '1234567890',
                      'run_id': 'run-vid-123',
                  }).encode('utf-8')
              ).decode('utf-8')
          )
      }
  }
  event = MockCloudEvent(
      attributes={
          'type': 'google.cloud.pubsub.topic.v1.messagePublished',
          'id': 'event-vid-123',
      },
      data=payload,
  )
  res, status = main.main(event)
  assert status == 200
  assert res == 'OK'
  mock_run.assert_called_once_with(
      date_partition='2026-09-10',
      customer_id='1234567890',
      run_id='run-vid-123',
      telemetry=mock.ANY,
  )


def test_main_cloudevent_invalid_schema_returns_400():
  """Tests CloudEvent entrypoint rejects invalid schema payload."""
  payload = {
      'message': {
          'data': (
              base64.b64encode(
                  json.dumps({'invalid_key': 'no_partition'}).encode('utf-8')
              ).decode('utf-8')
          )
      }
  }
  event = MockCloudEvent(
      attributes={
          'type': 'google.cloud.pubsub.topic.v1.messagePublished',
          'id': 'event-vid-123',
      },
      data=payload,
  )
  res, status = main.main(event)
  assert status == 400


@mock.patch.object(main, 'run')
def test_main_cloudevent_runtime_error_returns_500(mock_run):
  """Tests CloudEvent entrypoint handles unexpected runtime errors with 500."""
  mock_run.side_effect = RuntimeError('Unexpected YouTube API Outage')
  payload = {
      'message': {
          'data': (
              base64.b64encode(
                  json.dumps({'date_partition': '2026-09-10'}).encode('utf-8')
              ).decode('utf-8')
          )
      }
  }
  event = MockCloudEvent(
      attributes={
          'type': 'google.cloud.pubsub.topic.v1.messagePublished',
          'id': 'event-vid-123',
      },
      data=payload,
  )
  res, status = main.main(event)
  assert status == 500
  assert 'Internal Server Error' in res
