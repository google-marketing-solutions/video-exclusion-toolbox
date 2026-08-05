"""Unit tests for google-ads-accounts-dispatcher main module."""

# pylint: disable=protected-access

import importlib.util
import json
import pathlib
import sys
from unittest import mock

import flask

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
  mock_creds = mock.Mock()
  mock_creds.universe_domain = 'googleapis.com'
  mock_auth_default.return_value = (mock_creds, 'test_project')
  mock_sheets_service = mock.Mock()
  mock_build.return_value = mock_sheets_service

  mock_spreadsheets = mock.Mock()
  mock_sheets_service.spreadsheets.return_value = mock_spreadsheets

  def values_get_side_effect(**kwargs):
    """Mock side effect for spreadsheets.values.get."""
    mock_execute = mock.Mock()
    range_name = kwargs.get('range')
    if range_name == 'google_ads_customer_ids':
      mock_execute.execute.return_value = {
          'values': [
              ['123-456-7890', 'Enabled', '999-888-7777'],
              ['111-222-3333', 'Disabled', '444-555-6666'],
              ['723-928-2798', 'Disabled'],
              ['356-029-6721', 'Enabled'],
          ]
      }
    elif range_name == 'google_ads_filters':
      mock_execute.execute.return_value = {'values': [['clicks', '>', '5']]}
    elif range_name == 'google_ads_lookback_days':
      mock_execute.execute.return_value = {'values': [['90']]}
    elif range_name == 'configuration':
      mock_execute.execute.return_value = {
          'values': [['foo', 'bar'], ['baz', 'qux']]
      }
    return mock_execute

  mock_spreadsheets.values.return_value.get.side_effect = values_get_side_effect

  result = main._get_config_from_sheet('test_sheet_123')

  assert len(result) == 2
  assert result[0] == {
      'sheet_id': 'test_sheet_123',
      'customer_id': '1234567890',
      'mcc_for_exclusions': '9998887777',
      'lookback_days': 90,
      'gads_filters': 'metrics.clicks > 5',
      'settings': {'foo': 'bar', 'baz': 'qux'},
  }
  assert result[1] == {
      'sheet_id': 'test_sheet_123',
      'customer_id': '3560296721',
      'mcc_for_exclusions': '',
      'lookback_days': 90,
      'gads_filters': 'metrics.clicks > 5',
      'settings': {'foo': 'bar', 'baz': 'qux'},
  }


@mock.patch.object(main.discovery, 'build')
@mock.patch.object(main.google.auth, 'default')
def test_get_config_from_sheet_empty_lookback_days_defaults_to_1(
    mock_auth_default, mock_build
):
  mock_creds = mock.Mock()
  mock_creds.universe_domain = 'googleapis.com'
  mock_auth_default.return_value = (mock_creds, 'test_project')
  mock_sheets_service = mock.Mock()
  mock_build.return_value = mock_sheets_service
  mock_spreadsheets = mock.Mock()
  mock_sheets_service.spreadsheets.return_value = mock_spreadsheets

  def values_get_side_effect(**kwargs):
    """Mock side effect for spreadsheets.values.get."""
    mock_execute = mock.Mock()
    range_name = kwargs.get('range')
    if range_name == 'google_ads_customer_ids':
      mock_execute.execute.return_value = {
          'values': [['1234567890', 'Enabled', '9998887777']]
      }
    elif range_name == 'google_ads_lookback_days':
      mock_execute.execute.return_value = {'values': []}
    else:
      mock_execute.execute.return_value = {'values': []}
    return mock_execute

  mock_spreadsheets.values.return_value.get.side_effect = values_get_side_effect

  result = main._get_config_from_sheet('test_sheet_123')
  assert len(result) == 1
  assert result[0]['lookback_days'] == 1


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
