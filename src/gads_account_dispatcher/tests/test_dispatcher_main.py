"""Unit tests for gads_account_dispatcher main module."""

# pylint: disable=protected-access

import importlib.util
import json
import pathlib
import sys
from unittest import mock

import flask
import google.auth.credentials

_project_dir = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    'dispatcher_main', _project_dir / 'main.py'
)
main = importlib.util.module_from_spec(_spec)
sys.modules['dispatcher_main'] = main
_spec.loader.exec_module(main)


def test_gads_filters_to_gaql_string_non_empty_list_returns_condition_string():
  filters = [
      ['clicks', '>', '10'],
      ['impressions', '>=', '100'],
  ]
  expected = 'metrics.clicks > 10 AND metrics.impressions >= 100'
  assert main._gads_filters_to_gaql_string(filters) == expected


def test_gads_filters_to_gaql_string_empty_list_returns_empty_string():
  assert not main._gads_filters_to_gaql_string([])


@mock.patch.object(main.discovery, 'build')
@mock.patch.object(main.google.auth, 'default')
def test_get_config_from_sheet_valid_sheet_returns_customer_configs(
    mock_auth_default, mock_build
):
  """Tests successfully fetching customer configurations from a valid sheet."""
  mock_creds = mock.create_autospec(
      google.auth.credentials.Credentials, instance=True
  )
  mock_creds.universe_domain = 'googleapis.com'
  mock_auth_default.return_value = (mock_creds, 'test_project')
  mock_sheets_service = mock.Mock()
  mock_build.return_value = mock_sheets_service

  mock_spreadsheets = mock.Mock()
  mock_sheets_service.spreadsheets.return_value = mock_spreadsheets

  # Input IDs contain hyphens; expected IDs should be sanitized without hyphens.
  raw_customer_id_1 = '123-456-7890'
  expected_customer_id_1 = '1234567890'
  raw_mcc_id_1 = '999-888-7777'
  expected_mcc_id_1 = '9998887777'

  raw_customer_id_2 = '356-029-6721'
  expected_customer_id_2 = '3560296721'

  raw_filter_row = ['clicks', '>', '5']
  expected_gads_filters = 'metrics.clicks > 5'

  raw_lookback_days = '90'
  expected_lookback_days = 90

  raw_configuration = [['foo', 'bar'], ['baz', 'qux']]
  expected_settings = {'foo': 'bar', 'baz': 'qux'}

  def values_get_side_effect(**kwargs):
    """Mock side effect for spreadsheets.values.get."""
    mock_execute = mock.Mock()
    range_name = kwargs.get('range')
    if range_name == 'google_ads_customer_ids':
      mock_execute.execute.return_value = {
          'values': [
              [raw_customer_id_1, 'Enabled', raw_mcc_id_1],
              ['111-222-3333', 'Disabled', '444-555-6666'],
              ['723-928-2798', 'Disabled'],
              [raw_customer_id_2, 'Enabled'],
          ]
      }
    elif range_name == 'google_ads_filters':
      mock_execute.execute.return_value = {'values': [raw_filter_row]}
    elif range_name == 'google_ads_lookback_days':
      mock_execute.execute.return_value = {'values': [[raw_lookback_days]]}
    elif range_name == 'configuration':
      mock_execute.execute.return_value = {'values': raw_configuration}
    return mock_execute

  mock_spreadsheets.values.return_value.get.side_effect = values_get_side_effect

  result = main._get_config_from_sheet('test_sheet_123')

  assert len(result) == 2
  assert result[0] == {
      'sheet_id': 'test_sheet_123',
      'customer_id': expected_customer_id_1,
      'mcc_for_exclusions': expected_mcc_id_1,
      'lookback_days': expected_lookback_days,
      'gads_filters': expected_gads_filters,
      'settings': expected_settings,
  }
  assert result[1] == {
      'sheet_id': 'test_sheet_123',
      'customer_id': expected_customer_id_2,
      'mcc_for_exclusions': '',
      'lookback_days': expected_lookback_days,
      'gads_filters': expected_gads_filters,
      'settings': expected_settings,
  }


@mock.patch.object(main.discovery, 'build')
@mock.patch.object(main.google.auth, 'default')
def test_get_config_from_sheet_empty_lookback_days_defaults_to_1(
    mock_auth_default, mock_build
):
  """Tests that lookback_days defaults to 1 when the sheet range is empty."""
  mock_creds = mock.create_autospec(
      google.auth.credentials.Credentials, instance=True
  )
  mock_creds.universe_domain = 'googleapis.com'
  mock_auth_default.return_value = (mock_creds, 'test_project')
  mock_sheets_service = mock.Mock()
  mock_build.return_value = mock_sheets_service
  mock_spreadsheets = mock.Mock()
  mock_sheets_service.spreadsheets.return_value = mock_spreadsheets

  raw_customer_id = '123-456-7890'
  raw_mcc_id = '999-888-7777'
  default_lookback_days = 1

  def values_get_side_effect(**kwargs):
    """Mock side effect for spreadsheets.values.get."""
    mock_execute = mock.Mock()
    range_name = kwargs.get('range')
    if range_name == 'google_ads_customer_ids':
      mock_execute.execute.return_value = {
          'values': [[raw_customer_id, 'Enabled', raw_mcc_id]]
      }
    elif range_name == 'google_ads_lookback_days':
      mock_execute.execute.return_value = {'values': []}
    else:
      mock_execute.execute.return_value = {'values': []}
    return mock_execute

  mock_spreadsheets.values.return_value.get.side_effect = values_get_side_effect

  result = main._get_config_from_sheet('test_sheet_123')
  assert len(result) == 1
  assert result[0]['lookback_days'] == default_lookback_days


@mock.patch.object(main, 'publish_batch')
@mock.patch.object(main, '_get_config_from_sheet')
def test_run_valid_sheet_fetches_config_and_publishes_batch(
    mock_get_config, mock_publish
):
  mock_get_config.return_value = [{'sheet_id': 's1'}]
  main.run('test_sheet_123')
  mock_get_config.assert_called_once_with('test_sheet_123')
  mock_publish.assert_called_once()


def test_main_http_valid_request_returns_200_success():
  app = flask.Flask(__name__)
  with app.test_request_context(
      json={'sheet_id': 'test_sheet_123'},
      method='POST',
  ):
    with mock.patch.object(main, 'run') as mock_run:
      response = main.main(flask.request)
      mock_run.assert_called_once_with('test_sheet_123')
      assert response.status_code == 200
      data = json.loads(response.get_data(as_text=True))
      assert data['status'] == 'Success'


def test_main_http_schema_validation_error_returns_400():
  app = flask.Flask(__name__)
  with app.test_request_context(
      json={'invalid_key': 'foo'},
      method='POST',
  ):
    response = main.main(flask.request)
    assert response.status_code == 400
    data = json.loads(response.get_data(as_text=True))
    assert data['status'] == 'Failed'


def test_main_http_empty_request_body_returns_400():
  app = flask.Flask(__name__)
  with app.test_request_context(
      data='',
      method='POST',
      content_type='application/json',
  ):
    response = main.main(flask.request)
    assert response.status_code == 400
    data = json.loads(response.get_data(as_text=True))
    assert data['status'] == 'Failed'


def test_main_http_internal_server_error_returns_500():
  app = flask.Flask(__name__)
  with app.test_request_context(
      json={'sheet_id': 'test_sheet_123'},
      method='POST',
  ):
    with mock.patch.object(main, 'run', side_effect=RuntimeError('API error')):
      response = main.main(flask.request)
      assert response.status_code == 500
      data = json.loads(response.get_data(as_text=True))
      assert data['status'] == 'Failed'
      assert 'API error' in data['message']
