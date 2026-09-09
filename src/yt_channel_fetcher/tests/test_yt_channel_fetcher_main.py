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
"""Unit tests for the yt_channel_fetcher module."""

import base64
import json
from typing import Any
from unittest import mock

from googleapiclient import errors as googleapiclient_errors
import pytest
from yt_channel_fetcher import main


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
def test_get_unprocessed_channel_ids_with_partition_and_customer(
    mock_bq_client_cls,
):
  """Tests fetching unprocessed channel IDs with date_partition and customer_id filter."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job

  row1 = mock.MagicMock()
  row1.channel_id = 'UC_channel_1'
  row2 = mock.MagicMock()
  row2.channel_id = 'UC_channel_2'
  mock_query_job.result.return_value = [row1, row2]

  channel_ids = main.get_unprocessed_channel_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_channel',
      target_table='youtube_channel',
      date_partition='2026-09-07',
      customer_id='1234567890',
  )
  assert channel_ids == ['UC_channel_1', 'UC_channel_2']
  query_arg = mock_client.query.call_args[0][0]
  assert 'DATE(datetime_updated) = DATE(@date_partition)' in query_arg
  assert 'customer_id = @customer_id' in query_arg


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_channel_ids_backfill_all_omits_partition_filter(
    mock_bq_client_cls,
):
  """Tests that date_partition='all' omits the date partition filter for full backfills."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job
  mock_query_job.result.return_value = []

  channel_ids = main.get_unprocessed_channel_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_channel',
      target_table='youtube_channel',
      date_partition='all',
      customer_id=None,
  )
  assert channel_ids == []
  query_arg = mock_client.query.call_args[0][0]
  assert 'DATE(datetime_updated)' not in query_arg
  assert '@customer_id' not in query_arg


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_channel_ids_customer_all_omits_filter(
    mock_bq_client_cls,
):
  """Tests that customer_id='all' omits the customer filter."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job
  mock_query_job.result.return_value = []

  channel_ids = main.get_unprocessed_channel_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_channel',
      target_table='youtube_channel',
      date_partition='2026-09-07',
      customer_id='all',
  )
  assert channel_ids == []
  query_arg = mock_client.query.call_args[0][0]
  assert 'DATE(datetime_updated)' in query_arg
  assert '@customer_id' not in query_arg


@mock.patch.object(main.bigquery, 'Client')
def test_get_unprocessed_channel_ids_none_partition_omits_filter(
    mock_bq_client_cls,
):
  """Tests that date_partition=None omits the date partition filter."""
  mock_client = mock.MagicMock()
  mock_bq_client_cls.return_value = mock_client
  mock_query_job = mock.MagicMock()
  mock_client.query.return_value = mock_query_job
  mock_query_job.result.return_value = []

  channel_ids = main.get_unprocessed_channel_ids(
      bq_client=mock_client,
      project_id='test-project',
      dataset_id='test_dataset',
      source_table='google_ads_report_channel',
      target_table='youtube_channel',
      date_partition=None,
  )
  assert channel_ids == []
  query_arg = mock_client.query.call_args[0][0]
  assert 'DATE(datetime_updated)' not in query_arg


def test_fetch_channel_metadata_from_youtube_valid_response():
  """Tests parsing and enriching channels from YouTube API response with unified metadata."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.channels().list.return_value = mock_request

  mock_response = {
      'items': [{
          'id': 'UC_sample_channel_1',
          'snippet': {
              'title': 'Sample Channel',
              'description': 'Channel Description for Brand Safety',
              'customUrl': '@samplechannel',
              'country': 'US',
              'thumbnails': {
                  'high': {'url': 'https://yt3.ggpht.com/high.jpg'},
              },
          },
          'statistics': {
              'viewCount': '100000',
              'videoCount': '500',
              'subscriberCount': '25000',
          },
          'topicDetails': {
              'topicCategories': [
                  'https://en.wikipedia.org/wiki/Entertainment',
                  'https://en.wikipedia.org/wiki/Music',
              ]
          },
      }]
  }
  mock_request.execute.return_value = mock_response

  records = main.fetch_channel_metadata_from_youtube(
      youtube_service=mock_youtube,
      channel_ids=['UC_sample_channel_1'],
      now_iso='2026-09-07T12:00:00Z',
  )
  assert len(records) == 1
  rec = records[0]
  assert rec['channel_id'] == 'UC_sample_channel_1'
  assert rec['title'] == 'Sample Channel'
  assert rec['description'] == 'Channel Description for Brand Safety'
  assert rec['custom_url'] == '@samplechannel'
  assert rec['country'] == 'US'
  assert rec['thumbnail_url'] == 'https://yt3.ggpht.com/high.jpg'
  assert rec['view_count'] == 100000
  assert rec['video_count'] == 500
  assert rec['subscriber_count'] == 25000
  assert rec['clean_topics'] == ['Entertainment', 'Music']
  assert rec['datetime_updated'] == '2026-09-07T12:00:00Z'


def test_fetch_channel_metadata_from_youtube_tombstone_handling():
  """Tests tombstone generation for deleted or banned channels."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.channels().list.return_value = mock_request

  mock_request.execute.return_value = {'items': []}

  records = main.fetch_channel_metadata_from_youtube(
      youtube_service=mock_youtube,
      channel_ids=['UC_deleted_channel'],
      now_iso='2026-09-07T12:00:00Z',
  )
  assert len(records) == 1
  rec = records[0]
  assert rec['channel_id'] == 'UC_deleted_channel'
  assert rec['title'] == ''
  assert rec['description'] == ''
  assert rec['custom_url'] == ''
  assert rec['thumbnail_url'] == ''
  assert rec['view_count'] == 0
  assert rec['datetime_updated'] == '2026-09-07T12:00:00Z'


@mock.patch.object(main, 'get_unprocessed_channel_ids')
@mock.patch.object(main.bigquery, 'Client')
def test_run_no_channels_to_process_returns_zero(
    mock_bq_client_cls, mock_get_unprocessed
):
  """Tests run exits early and returns 0 when no unprocessed channels exist."""
  mock_get_unprocessed.return_value = []
  count = main.run(date_partition='2026-09-07', customer_id='1234567890')
  assert count == 0


@mock.patch.object(main, 'upsert_ndjson_to_bq')
@mock.patch.object(main, 'fetch_channel_metadata_from_youtube')
@mock.patch.object(main, '_build_youtube_service')
@mock.patch.object(main, 'get_unprocessed_channel_ids')
@mock.patch.object(main.bigquery, 'Client')
def test_run_successful_fetch_and_upsert(
    mock_bq_client_cls,
    mock_get_unprocessed,
    mock_build_yt,
    mock_fetch_yt,
    mock_upsert_bq,
):
  """Tests end-to-end run execution with unpartitioned upsert."""
  mock_get_unprocessed.return_value = ['UC_channel_1', 'UC_channel_2']
  mock_fetch_yt.return_value = [
      {
          'channel_id': 'UC_channel_1',
          'title': 'Chan 1',
          'view_count': 100,
          'video_count': 10,
          'subscriber_count': 5,
          'description': 'Desc 1',
          'custom_url': '@c1',
          'country': 'US',
          'thumbnail_url': 'http://thumb1.jpg',
          'topic_categories': [],
          'clean_topics': [],
          'datetime_updated': '2026-09-07T12:00:00Z',
      },
      {
          'channel_id': 'UC_channel_2',
          'title': 'Chan 2',
          'view_count': 200,
          'video_count': 20,
          'subscriber_count': 10,
          'description': 'Desc 2',
          'custom_url': '@c2',
          'country': 'GB',
          'thumbnail_url': 'http://thumb2.jpg',
          'topic_categories': [],
          'clean_topics': [],
          'datetime_updated': '2026-09-07T12:00:00Z',
      },
  ]

  count = main.run(
      date_partition='2026-09-07', customer_id='1234567890', run_id='test-run'
  )
  assert count == 2
  mock_upsert_bq.assert_called_once()
  assert mock_upsert_bq.call_args.kwargs['partition_date'] is None
  assert mock_upsert_bq.call_args.kwargs['key_columns'] == ['channel_id']


def test_fetch_channel_metadata_from_youtube_handles_null_statistics():
  """Tests that null or missing statistics fields are safely converted to 0."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.channels().list.return_value = mock_request

  mock_request.execute.return_value = {
      'items': [{
          'id': 'UC_null_stats',
          'snippet': {
              'title': 'Null Stats Channel',
              'description': 'Description',
              'customUrl': '@nullstats',
              'country': 'US',
              'thumbnails': {'high': {'url': 'https://yt3.ggpht.com/high.jpg'}},
          },
          'statistics': {
              'viewCount': None,
              'videoCount': None,
              'subscriberCount': None,
          },
          'topicDetails': {
              'topicCategories': [
                  'https://en.wikipedia.org/wiki/Entertainment/'
              ],
          },
      }]
  }

  records = main.fetch_channel_metadata_from_youtube(
      youtube_service=mock_youtube,
      channel_ids=['UC_null_stats'],
      now_iso='2026-09-07T12:00:00Z',
  )
  assert len(records) == 1
  rec = records[0]
  assert rec['view_count'] == 0
  assert rec['video_count'] == 0
  assert rec['subscriber_count'] == 0
  assert rec['clean_topics'] == ['Entertainment']


def test_fetch_channel_metadata_from_youtube_raises_http_error():
  """Tests that YouTube API HttpError is propagated."""
  mock_youtube = mock.MagicMock()
  mock_request = mock.MagicMock()
  mock_youtube.channels().list.return_value = mock_request

  resp = mock.MagicMock(status=503, reason='Backend Error')
  mock_request.execute.side_effect = googleapiclient_errors.HttpError(
      resp=resp, content=b'Backend Error'
  )

  with pytest.raises(googleapiclient_errors.HttpError):
    main.fetch_channel_metadata_from_youtube(
        youtube_service=mock_youtube,
        channel_ids=['UC_err'],
        now_iso='2026-09-07T12:00:00Z',
    )


@mock.patch.object(main, 'run')
def test_main_cloudevent_valid_payload_returns_200(mock_run):
  """Tests CloudEvent entrypoint with valid payload."""
  payload = {
      'message': {
          'data': (
              base64.b64encode(
                  json.dumps({
                      'date_partition': '2026-09-07',
                      'customer_id': '1234567890',
                      'run_id': 'run-123',
                  }).encode('utf-8')
              ).decode('utf-8')
          )
      }
  }
  event = MockCloudEvent(
      attributes={
          'type': 'google.cloud.pubsub.topic.v1.messagePublished',
          'id': 'event-123',
      },
      data=payload,
  )
  res, status = main.main(event)
  assert status == 200
  assert res == 'OK'
  mock_run.assert_called_once_with(
      date_partition='2026-09-07',
      customer_id='1234567890',
      run_id='run-123',
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
          'id': 'event-123',
      },
      data=payload,
  )
  res, status = main.main(event)
  assert status == 400


@mock.patch.object(main, 'run')
def test_main_cloudevent_runtime_error_returns_500(mock_run):
  """Tests CloudEvent entrypoint handles unexpected runtime errors with 500."""
  mock_run.side_effect = RuntimeError('YouTube API Quota Exceeded')
  payload = {
      'message': {
          'data': (
              base64.b64encode(
                  json.dumps({'date_partition': '2026-09-07'}).encode('utf-8')
              ).decode('utf-8')
          )
      }
  }
  event = MockCloudEvent(
      attributes={
          'type': 'google.cloud.pubsub.topic.v1.messagePublished',
          'id': 'event-123',
      },
      data=payload,
  )
  res, status = main.main(event)
  assert status == 500
  assert 'Internal Server Error' in res
