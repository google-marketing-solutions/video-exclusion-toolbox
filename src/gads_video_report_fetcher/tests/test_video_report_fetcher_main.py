"""Unit tests for the gads_video_report_fetcher main module."""

import base64
import io
import json
from typing import Any
from unittest import mock

from gads_video_report_fetcher import main


class MockCloudEvent:
  """Mock CloudEvent object conforming to functions_framework CloudEvent protocol."""

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


def test_get_report_query_single_day_without_filters():
  """Tests query formatting for 1 lookback day without custom filters."""
  query = main.get_report_query(lookback_days=1)
  assert 'AND segments.date = ' in query
  assert 'detail_placement_view.placement_type = "YOUTUBE_VIDEO"' in query


def test_get_report_query_multi_day_with_filters():
  """Tests query formatting for multi-day lookback with custom GAQL filters."""
  query = main.get_report_query(
      lookback_days=14, gads_filters='metrics.clicks > 10'
  )
  assert 'BETWEEN' in query
  assert 'AND metrics.clicks > 10' in query


@mock.patch.object(main.googleads_client, 'GoogleAdsClient')
@mock.patch.object(main.google.auth, 'default')
def test_fetch_and_stream_report_records_valid_stream_returns_ndjson(
    mock_auth_default,
    mock_gads_client_cls,
):
  """Tests stream parsing directly into an NDJSON buffer."""
  mock_auth_default.return_value = (mock.MagicMock(), 'project_id')
  mock_client = mock.MagicMock()
  mock_ga_service = mock.MagicMock()
  mock_gads_client_cls.return_value = mock_client
  mock_client.get_service.return_value = mock_ga_service
  mock_client.get_type.return_value = mock.MagicMock()

  # Create mock batch results
  mock_row = mock.MagicMock()
  mock_row.customer.id = '1234567890'
  mock_row.campaign.id = 111
  mock_row.campaign.name = 'Camp 1'
  mock_row.ad_group.id = 222
  mock_row.ad_group.name = 'AdGroup 1'
  mock_row.detail_placement_view.placement = 'video_id_1'
  mock_row.detail_placement_view.display_name = 'Test Video 1'
  mock_row.detail_placement_view.target_url = (
      'https://youtube.com/watch?v=video_id_1'
  )
  mock_row.detail_placement_view.placement_type = 'YOUTUBE_VIDEO'
  mock_row.detail_placement_view.group_placement_target_url = (
      'https://youtube.com/channel/chan_1'
  )
  mock_row.metrics.impressions = 100
  mock_row.metrics.cost_micros = 5000000
  mock_row.metrics.conversions = 2
  mock_row.metrics.video_trueview_view_rate = 0.25
  mock_row.metrics.video_trueview_views = 25
  mock_row.metrics.clicks = 5
  mock_row.metrics.average_cpm = 50.0
  mock_row.metrics.ctr = 0.05
  mock_row.metrics.all_conversions_from_interactions_rate = 0.4
  mock_row.metrics.video_quartile_p25_rate = 0.8
  mock_row.metrics.video_quartile_p50_rate = 0.6
  mock_row.metrics.video_quartile_p75_rate = 0.4
  mock_row.metrics.video_quartile_p100_rate = 0.2

  mock_batch = mock.MagicMock()
  mock_batch.results = [mock_row]
  mock_ga_service.search_stream.return_value = [mock_batch]

  # pylint: disable=protected-access
  buffer, retrieved_video_ids, row_count = (
      main._fetch_and_stream_report_records('1234567890', 1, '')
  )
  assert row_count == 1
  assert 'video_id_1' in retrieved_video_ids
  lines = buffer.getvalue().decode('utf-8').strip().split('\n')
  assert len(lines) == 1
  record = json.loads(lines[0])
  assert record['video_id'] == 'video_id_1'
  assert record['customer_id'] == '1234567890'
  assert record['campaign_id'] == 111
  assert record['campaign_name'] == 'Camp 1'
  assert record['ad_group_id'] == 222
  assert record['ad_group_name'] == 'AdGroup 1'
  assert record['impressions'] == 100
  assert record['ctr'] == 0.05
  assert record['average_cpm'] == 50000000.0
  assert record['video_view_rate'] == 0.25


@mock.patch.object(main.googleads_client, 'GoogleAdsClient')
@mock.patch.object(main.google.auth, 'default')
def test_fetch_and_stream_report_records_multi_day_recalculates_rates(
    mock_auth_default,
    mock_gads_client_cls,
):
  """Tests that multi-day rows for same composite key correctly sum metrics and recompute rates."""
  mock_auth_default.return_value = (mock.MagicMock(), 'project_id')
  mock_client = mock.MagicMock()
  mock_ga_service = mock.MagicMock()
  mock_gads_client_cls.return_value = mock_client
  mock_client.get_service.return_value = mock_ga_service
  mock_client.get_type.return_value = mock.MagicMock()

  # Day 1 row
  row1 = mock.MagicMock()
  row1.customer.id = '1234567890'
  row1.campaign.id = 100
  row1.campaign.name = 'Camp'
  row1.ad_group.id = 200
  row1.ad_group.name = 'AdGroup'
  row1.detail_placement_view.placement = 'video_x'
  row1.detail_placement_view.display_name = 'Vid X'
  row1.detail_placement_view.target_url = 'url_x'
  row1.detail_placement_view.placement_type = 'YOUTUBE_VIDEO'
  row1.detail_placement_view.group_placement_target_url = 'chan_url'
  row1.metrics.impressions = 1000
  row1.metrics.cost_micros = 10000000  # 10 USD
  row1.metrics.conversions = 5
  row1.metrics.video_trueview_views = 200
  row1.metrics.clicks = 50

  # Day 2 row (same placement, different metrics)
  row2 = mock.MagicMock()
  row2.customer.id = '1234567890'
  row2.campaign.id = 100
  row2.campaign.name = 'Camp'
  row2.ad_group.id = 200
  row2.ad_group.name = 'AdGroup'
  row2.detail_placement_view.placement = 'video_x'
  row2.detail_placement_view.display_name = 'Vid X'
  row2.detail_placement_view.target_url = 'url_x'
  row2.detail_placement_view.placement_type = 'YOUTUBE_VIDEO'
  row2.detail_placement_view.group_placement_target_url = 'chan_url'
  row2.metrics.impressions = 1000
  row2.metrics.cost_micros = 30000000  # 30 USD
  row2.metrics.conversions = 15
  row2.metrics.video_trueview_views = 400
  row2.metrics.clicks = 150

  mock_batch = mock.MagicMock()
  mock_batch.results = [row1, row2]
  mock_ga_service.search_stream.return_value = [mock_batch]

  # pylint: disable=protected-access
  buffer, retrieved_video_ids, row_count = (
      main._fetch_and_stream_report_records('1234567890', 7, '')
  )
  assert row_count == 1
  assert 'video_x' in retrieved_video_ids
  record = json.loads(buffer.getvalue().decode('utf-8').strip())

  # Total: impressions=2000, cost=40,000,000, clicks=200, views=600
  assert record['impressions'] == 2000
  assert record['cost_micros'] == 40000000
  assert record['clicks'] == 200
  assert record['video_views'] == 600
  assert record['conversions'] == 20
  # Recalculated rates:
  assert record['ctr'] == 200.0 / 2000.0  # 0.10
  assert (
      record['average_cpm'] == (40000000.0 / 2000.0) * 1000.0
  )  # 20,000,000 micros = $20 CPM
  assert record['video_view_rate'] == 600.0 / 2000.0  # 0.30


@mock.patch.object(main.time, 'sleep')
@mock.patch.object(main.googleads_client, 'GoogleAdsClient')
@mock.patch.object(main.google.auth, 'default')
def test_fetch_and_stream_report_records_retries_on_transient_stream_error(
    mock_auth_default,
    mock_gads_client_cls,
    mock_sleep,
):
  """Tests that transient error mid-stream triggers retry and succeeds on subsequent attempt."""
  mock_auth_default.return_value = (mock.MagicMock(), 'project_id')
  mock_client = mock.MagicMock()
  mock_ga_service = mock.MagicMock()
  mock_gads_client_cls.return_value = mock_client
  mock_client.get_service.return_value = mock_ga_service
  mock_client.get_type.return_value = mock.MagicMock()

  # Attempt 1 raises transient GoogleAdsException
  transient_error = main.gads_errors.GoogleAdsException(
      mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), 'test_request_id'
  )
  transient_error.args = ('UNAVAILABLE: Channel disconnected',)

  # Attempt 2 succeeds
  mock_row = mock.MagicMock()
  mock_row.customer.id = '1234567890'
  mock_row.campaign.id = 111
  mock_row.ad_group.id = 222
  mock_row.detail_placement_view.placement = 'video_retry_1'
  mock_row.detail_placement_view.display_name = 'Vid'
  mock_row.detail_placement_view.target_url = 'url'
  mock_row.detail_placement_view.placement_type = 'YOUTUBE_VIDEO'
  mock_row.detail_placement_view.group_placement_target_url = 'chan'
  mock_row.metrics.impressions = 50
  mock_row.metrics.cost_micros = 1000
  mock_row.metrics.conversions = 0
  mock_row.metrics.video_trueview_views = 10
  mock_row.metrics.clicks = 2

  mock_batch = mock.MagicMock()
  mock_batch.results = [mock_row]

  mock_ga_service.search_stream.side_effect = [
      transient_error,
      [mock_batch],
  ]

  # pylint: disable=protected-access
  buffer, retrieved_video_ids, row_count = (
      main._fetch_and_stream_report_records(
          '1234567890', 1, '', max_retries=3, base_backoff_seconds=0.01
      )
  )
  assert row_count == 1
  assert 'video_retry_1' in retrieved_video_ids
  assert buffer.getvalue()
  mock_sleep.assert_called_once()


@mock.patch.object(main, 'publish_batch')
@mock.patch.object(main, 'upsert_ndjson_to_bq')
@mock.patch.object(main, 'get_existing_partition_ids')
@mock.patch.object(main.bigquery, 'Client')
@mock.patch.object(main, '_fetch_and_stream_report_records')
def test_run_new_records_persists_to_bq_and_publishes_event(
    mock_fetch_records,
    mock_bq_client_cls,
    mock_get_existing_ids,
    mock_upsert_bq,
    mock_publish_batch,
):
  """Tests complete run flow when new, non-duplicate records are found."""
  del mock_bq_client_cls
  fake_buffer = io.BytesIO(b'{"video_id": "v1"}\n{"video_id": "v2"}\n')
  mock_fetch_records.return_value = (fake_buffer, {'v1', 'v2'}, 2)
  mock_get_existing_ids.return_value = {'v1'}

  mock_telemetry = mock.Mock()
  main.run(
      customer_id='1234567890',
      lookback_days=1,
      gads_filters='',
      telemetry=mock_telemetry,
  )

  mock_upsert_bq.assert_called_once()
  mock_publish_batch.assert_called_once()
  assert mock_telemetry.log_step.call_count == 5


@mock.patch.object(main, 'publish_batch')
@mock.patch.object(main, 'upsert_ndjson_to_bq')
@mock.patch.object(main, '_fetch_and_stream_report_records')
def test_run_empty_report_exits_early_without_bq_or_pubsub(
    mock_fetch_records, mock_upsert_bq, mock_publish_batch
):
  """Tests that an empty report stream exits cleanly without writing or publishing."""
  mock_fetch_records.return_value = (io.BytesIO(), set(), 0)

  mock_telemetry = mock.Mock()
  main.run(
      customer_id='1234567890',
      lookback_days=1,
      gads_filters='',
      telemetry=mock_telemetry,
  )

  mock_upsert_bq.assert_not_called()
  mock_publish_batch.assert_not_called()
  mock_telemetry.log_step.assert_called_with(
      step='SKIPPED',
      records_out=0,
      metadata={
          'reason': 'No placements returned from Google Ads or account disabled'
      },
  )


@mock.patch.object(main, 'run')
def test_main_cloudevent_valid_payload_executes_run(mock_run):
  """Tests that a valid CloudEvent pubsub message invokes run successfully."""
  payload = {
      'customer_id': '123-456-7890',
      'lookback_days': 7,
      'gads_filters': 'metrics.impressions > 50',
  }
  encoded_data = base64.b64encode(json.dumps(payload).encode('utf-8')).decode(
      'utf-8'
  )
  event = MockCloudEvent(data={'message': {'data': encoded_data}})

  main.main(event)

  mock_run.assert_called_once_with(
      customer_id='1234567890',
      lookback_days=7,
      gads_filters='metrics.impressions > 50',
      telemetry=mock.ANY,
  )


@mock.patch.object(main, 'run')
def test_main_cloudevent_schema_validation_error_handled_gracefully(mock_run):
  """Tests that schema validation failure is handled without re-raising or crashing."""
  invalid_payload = {
      'invalid_key': 'no_customer_id',
  }
  encoded_data = base64.b64encode(
      json.dumps(invalid_payload).encode('utf-8')
  ).decode('utf-8')
  event = MockCloudEvent(data={'message': {'data': encoded_data}})

  # Should not raise exception
  main.main(event)
  mock_run.assert_not_called()


@mock.patch.object(
    main, 'run', side_effect=RuntimeError('Database unreachable')
)
def test_main_cloudevent_runtime_exception_handled_gracefully(mock_run):
  """Tests that unexpected runtime exceptions are logged and handled safely."""
  payload = {
      'customer_id': '1234567890',
      'lookback_days': 1,
      'gads_filters': '',
  }
  encoded_data = base64.b64encode(json.dumps(payload).encode('utf-8')).decode(
      'utf-8'
  )
  event = MockCloudEvent(data={'message': {'data': encoded_data}})

  # Should catch and log error, avoiding uncaught crash
  main.main(event)
  mock_run.assert_called_once()


@mock.patch.object(main, 'run')
def test_main_cloudevent_extracts_event_id_for_telemetry(mock_run):
  """Tests that CloudEvent id is extracted via dictionary interface and assigned to telemetry."""
  payload = {
      'customer_id': '123-456-7890',
      'lookback_days': 1,
      'gads_filters': '',
  }
  encoded_data = base64.b64encode(json.dumps(payload).encode('utf-8')).decode(
      'utf-8'
  )
  event = MockCloudEvent(
      data={'message': {'data': encoded_data}},
      attributes={'id': 'cloudevent-test-id-12345'},
  )

  main.main(event)

  mock_run.assert_called_once()
  telemetry_arg = mock_run.call_args[1]['telemetry']
  assert telemetry_arg.run_id == 'cloudevent-test-id-12345'
