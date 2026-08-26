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

"""Unit tests for vet_common shared utilities."""

import base64
from concurrent import futures
import datetime
import io
import json
import logging
import os
import time
from typing import Any
from unittest import mock

from google.cloud import bigquery
from google.cloud import pubsub_v1
import pytest
from vet_common.bq import get_existing_partition_ids
from vet_common.bq import upsert_ndjson_to_bq
from vet_common.bq import write_ndjson_to_bq
from vet_common.dates import get_lookback_date_range
from vet_common.events import parse_pubsub_cloudevent
from vet_common.gads import DEFAULT_GOOGLE_ADS_API_VERSION
from vet_common.gads import get_google_ads_client_version
from vet_common.ids import sanitize_gads_id
from vet_common.logging import get_service_logger
from vet_common.logging import PipelineTelemetryContext
from vet_common.pubsub import publish_batch


class MockCloudEvent:
  """Mock CloudEvent object conforming to functions_framework CloudEvent protocol."""

  def __init__(self, data: dict[str, Any]):
    self.data = data


@pytest.mark.parametrize(
    ('raw_id', 'expected'),
    [
        ('123-456-7890', '1234567890'),
        ('805-652-0078', '8056520078'),
        (1234567890, '1234567890'),
        ('  123-456-7890  ', '1234567890'),
        ('n/a', ''),
        ('N/A', ''),
        ('none', ''),
        (None, ''),
    ],
)
def test_sanitize_gads_id_various_formats_returns_sanitized_string(
    raw_id, expected
):
  """Test sanitizing Google Ads customer and login IDs."""
  assert sanitize_gads_id(raw_id) == expected


def test_get_service_logger_local_test_returns_service_named_logger():
  """Test logger configuration in local test environment."""
  with mock.patch.dict(
      os.environ, {'IS_LOCAL_TEST': 'True', 'K_SERVICE': 'test-service'}
  ):
    logger = get_service_logger()
    assert logger.name == 'test-service'


@mock.patch.object(pubsub_v1, 'PublisherClient', autospec=True)
def test_publish_batch_valid_messages_publishes_all_messages(
    mock_publisher_client_cls,
):
  """Test publishing a batch of messages to Pub/Sub."""
  mock_publisher = mock_publisher_client_cls.return_value
  mock_publisher.topic_path.return_value = (
      'projects/test-project/topics/test-topic'
  )

  mock_future = futures.Future()
  mock_future.set_result('msg-id-123')
  mock_publisher.publish.return_value = mock_future

  messages = [{'foo': 'bar'}, {'baz': 123}]
  publish_batch('test-project', 'test-topic', messages)

  assert mock_publisher.publish.call_count == 2


def test_get_lookback_date_range_calculates_correct_start_and_end():
  """Test lookback date calculation."""
  fixed_today = datetime.date(2026, 8, 14)
  start, end = get_lookback_date_range(7, today=fixed_today)
  assert start == '2026-08-07'
  assert end == '2026-08-14'


def test_parse_pubsub_cloudevent_valid_payload_returns_dict():
  """Test parsing a valid CloudEvent pubsub payload."""
  payload = {'customer_id': '1234567890', 'lookback_days': 1}
  encoded = base64.b64encode(json.dumps(payload).encode('utf-8')).decode(
      'utf-8'
  )
  event = MockCloudEvent(data={'message': {'data': encoded}})

  schema = {
      'type': 'object',
      'properties': {'customer_id': {'type': 'string'}},
      'required': ['customer_id'],
  }
  result = parse_pubsub_cloudevent(event, schema=schema)
  assert result == payload


def test_parse_pubsub_cloudevent_schema_violation_returns_none():
  """Test that a schema validation error returns None and logs error."""
  payload = {'invalid_field': 123}
  encoded = base64.b64encode(json.dumps(payload).encode('utf-8')).decode(
      'utf-8'
  )
  event = MockCloudEvent(data={'message': {'data': encoded}})

  schema = {
      'type': 'object',
      'properties': {'customer_id': {'type': 'string'}},
      'required': ['customer_id'],
  }
  result = parse_pubsub_cloudevent(event, schema=schema)
  assert result is None


def test_parse_pubsub_cloudevent_empty_or_malformed_returns_none():
  """Test that invalid base64 or non-dict payloads return None."""
  event_empty = MockCloudEvent(data={})
  assert parse_pubsub_cloudevent(event_empty) is None

  event_invalid_json = MockCloudEvent(
      data={'message': {'data': base64.b64encode(b'not-json').decode('utf-8')}}
  )
  assert parse_pubsub_cloudevent(event_invalid_json) is None




def test_write_ndjson_to_bq_executes_load_job():
  """Test writing NDJSON buffer to BigQuery."""
  mock_client = mock.MagicMock(spec=bigquery.Client)
  mock_job = mock.MagicMock()
  mock_job.output_rows = 2
  mock_client.load_table_from_file.return_value = mock_job

  buf = io.BytesIO(b'{"a": 1}\n{"a": 2}\n')
  rows = write_ndjson_to_bq(mock_client, buf, 'proj.dataset.table')

  mock_client.load_table_from_file.assert_called_once()
  mock_job.result.assert_called_once()
  assert rows == 2


def test_upsert_ndjson_to_bq_creates_staging_merges_and_drops():
  """Test upserting NDJSON buffer into BigQuery via staging table and partition MERGE."""
  mock_client = mock.MagicMock(spec=bigquery.Client)
  mock_table = mock.MagicMock()
  mock_table.schema = [
      bigquery.SchemaField('customer_id', 'STRING'),
      bigquery.SchemaField('video_id', 'STRING'),
      bigquery.SchemaField('impressions', 'INTEGER'),
  ]
  mock_client.get_table.return_value = mock_table

  mock_load_job = mock.MagicMock()
  mock_load_job.output_rows = 5
  mock_client.load_table_from_file.return_value = mock_load_job

  mock_merge_job = mock.MagicMock()
  mock_client.query.return_value = mock_merge_job

  buf = io.BytesIO(
      b'{"customer_id": "c1", "video_id": "v1", "impressions": 10}\n'
  )

  rows = upsert_ndjson_to_bq(
      client=mock_client,
      ndjson_buffer=buf,
      project_id='test-proj',
      dataset_id='test-ds',
      table_name='GoogleAdsReportVideo',
      key_columns=['customer_id', 'video_id'],
      partition_date=datetime.date(2026, 8, 17),
  )

  assert rows == 5
  mock_client.create_table.assert_called_once()
  mock_client.load_table_from_file.assert_called_once()
  mock_client.query.assert_called_once()
  mock_client.delete_table.assert_called_once()


def test_get_existing_partition_ids_returns_set_of_ids():
  """Test querying distinct existing partition IDs from BigQuery."""
  mock_client = mock.MagicMock(spec=bigquery.Client)
  mock_query_job = mock.MagicMock()
  mock_query_job.result.return_value = [
      {'video_id': 'v1'},
      {'video_id': 'v2'},
  ]
  mock_client.query.return_value = mock_query_job

  ids = get_existing_partition_ids(
      mock_client,
      'test-proj',
      'test-ds',
      'GoogleAdsReportVideo',
      'video_id',
      datetime.date(2026, 8, 14),
  )

  assert ids == {'v1', 'v2'}
  mock_client.query.assert_called_once()


def test_get_google_ads_client_version_defaults_to_v25():
  """Test that get_google_ads_client_version returns DEFAULT_GOOGLE_ADS_API_VERSION when unset."""
  with mock.patch.dict(os.environ, {}, clear=True):
    assert get_google_ads_client_version() == DEFAULT_GOOGLE_ADS_API_VERSION
    assert DEFAULT_GOOGLE_ADS_API_VERSION == 'v25'


def test_get_google_ads_client_version_reads_env_variable():
  """Test that get_google_ads_client_version prioritizes GOOGLE_ADS_CLIENT_VERSION env var."""
  with mock.patch.dict(os.environ, {'GOOGLE_ADS_CLIENT_VERSION': 'v26'}):
    assert get_google_ads_client_version() == 'v26'


def test_pipeline_telemetry_context_initialization_and_success_logging():
  """Test PipelineTelemetryContext logging a structured step."""
  mock_logger = mock.MagicMock(spec=logging.Logger)
  telemetry = PipelineTelemetryContext(
      logger=mock_logger,
      service_name='test-service',
      customer_id='1234567890',
      run_id='run-123',
  )

  assert telemetry.service == 'test-service'
  assert telemetry.customer_id == '1234567890'
  assert telemetry.run_id == 'run-123'

  time.sleep(0.01)
  payload = telemetry.log_step(
      step='TEST_STEP',
      records_in=10,
      records_out=5,
      metadata={'extra': 'value'},
  )

  assert payload['event_type'] == 'pipeline_step'
  assert payload['run_id'] == 'run-123'
  assert payload['service'] == 'test-service'
  assert payload['customer_id'] == '1234567890'
  assert payload['step'] == 'TEST_STEP'
  assert payload['status'] == 'SUCCESS'
  assert payload['records_in'] == 10
  assert payload['records_out'] == 5
  assert payload['metadata'] == {'extra': 'value'}
  assert payload['step_duration_ms'] >= 5
  assert payload['elapsed_ms'] >= 5

  mock_logger.info.assert_called_once()
  logged_json = json.loads(mock_logger.info.call_args[0][0])
  assert logged_json == payload


def test_pipeline_telemetry_context_failure_logging():
  """Test PipelineTelemetryContext logging a failed step."""
  mock_logger = mock.MagicMock(spec=logging.Logger)
  telemetry = PipelineTelemetryContext(logger=mock_logger)
  telemetry.set_customer_id('8056520078')

  err = ValueError('Invalid argument provided')
  payload = telemetry.log_step(
      step='FAIL_STEP',
      status='FAILED',
      error=err,
  )

  assert payload['status'] == 'FAILED'
  assert payload['customer_id'] == '8056520078'
  assert payload['error_type'] == 'ValueError'
  assert payload['error_message'] == 'Invalid argument provided'

  mock_logger.error.assert_called_once()
  logged_json = json.loads(mock_logger.error.call_args[0][0])
  assert logged_json == payload
