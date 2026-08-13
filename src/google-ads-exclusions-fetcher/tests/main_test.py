# Copyright 2025 Google LLC
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

"""Unit tests for the main.py module."""

import base64
import datetime
import json
import os
from typing import Any, Callable
from unittest import mock

from conftest import TEST_CLIENT_VERSION
from conftest import TEST_CUSTOMER_ID_1
from conftest import TEST_DATASET_ID
from conftest import TEST_DEV_TOKEN
from conftest import TEST_LOGIN_CUSTOMER_ID
from conftest import TEST_MCC_EXCLUSION_ID_1
from conftest import TEST_PROJECT_ID
from conftest import TEST_SHEET_ID
from conftest import TEST_TABLE_ID
from google.ads.googleads import errors as gads_errors
from google.api_core import exceptions as api_exceptions
import main
from main import _get_config_from_sheet
from main import EXPECTED_LIST_NAMES
from main import main as main_function
from main import REQUEST_SCHEMA
from main import run as run_script
import pytest


def create_mock_cloud_event_data(payload: dict[str, Any]) -> dict[str, Any]:
  """Encodes payload for a mock Pub/Sub message."""
  return {
      'message': {'data': base64.b64encode(json.dumps(payload).encode('utf-8'))}
  }


@pytest.fixture(name='sheets_api_mock_setup')
def fixture_sheets_api_mock_setup(
    mocker: mock.Mock,
) -> tuple[mock.Mock, Callable[..., Any]]:
  """Patches 'main.discovery.build' and provides a way to set sheet data.

  Args:
    mocker: The mocker object to use for patching.

  Returns:
    A tuple containing:
      - mock_values_api: Mock for the Sheets API values object.
      - create_get_side_effect: Function to create a side_effect for get().
  """
  mock_discovery_build = mocker.patch('main.discovery.build')
  mock_sheets_service = mock.MagicMock(name='MockSheetsService')
  mock_spreadsheets = mock.MagicMock(name='MockSpreadsheets')
  mock_values_api = mock.MagicMock(name='MockValuesAPIObject')

  mock_discovery_build.return_value = mock_sheets_service
  mock_sheets_service.spreadsheets.return_value = mock_spreadsheets
  mock_spreadsheets.values.return_value = mock_values_api

  def _create_get_side_effect_for_test(
      sheet_data_map_for_current_test: dict[str, Any],
  ) -> Callable[..., Any]:
    """Creates the side_effect function for mock_values_api.get()."""

    def _actual_side_effect_func(*_args: Any, **kwargs: Any) -> mock.Mock:
      range_name = kwargs.get('range')
      mock_get_request = mock.MagicMock(name=f'MockGetRequest_{range_name}')
      execute_result_dict: dict[str, Any] = {}

      if range_name in sheet_data_map_for_current_test:
        range_data = sheet_data_map_for_current_test[range_name]
        if isinstance(range_data, dict) and 'values' in range_data:
          execute_result_dict['values'] = range_data['values']

      mock_get_request.execute.return_value = execute_result_dict
      return mock_get_request

    return _actual_side_effect_func

  return mock_values_api, _create_get_side_effect_for_test


@pytest.fixture(name='mock_credentials')
def fixture_mock_credentials(mocker: mock.Mock) -> mock.Mock:
  """Mocks google.auth.default and returns mock credentials."""
  mock_auth_default = mocker.patch('main.google.auth.default')
  mock_creds = mock.MagicMock(name='MockCredentials')
  mock_creds.universe_domain = 'googleapis.com'
  mock_creds.create_scoped.return_value = mock_creds
  mock_creds.authorize.side_effect = lambda http: http
  mock_auth_default.return_value = (mock_creds, TEST_PROJECT_ID)
  return mock_creds


@pytest.fixture(name='mock_gads_client_init')
def fixture_mock_gads_client_init(mocker: mock.Mock) -> mock.Mock:
  """Mocks GoogleAdsClient constructor and returns the mock class."""
  return mocker.patch('main.gads_client.GoogleAdsClient')


@pytest.fixture(name='mock_ads_client_instance')
def fixture_mock_ads_client_instance(
    mock_gads_client_init: mock.Mock,
) -> mock.Mock:
  """Provides a mock instance of GoogleAdsClient."""
  instance = mock.MagicMock(name='MockAdsClientInstance')
  mock_gads_client_init.return_value = instance
  return instance


@pytest.fixture(name='mock_bq_client_init')
def fixture_mock_bq_client_init(mocker: mock.Mock) -> mock.Mock:
  """Mocks bigquery.Client constructor and returns the mock class."""
  return mocker.patch('main.bigquery.Client')


@pytest.fixture(name='mock_bq_client_instance')
def fixture_mock_bq_client_instance(
    mock_bq_client_init: mock.Mock,
) -> mock.Mock:
  """Provides a mock instance of bigquery.Client."""
  instance = mock.MagicMock(name='MockBQClientInstance')
  mock_bq_client_init.return_value = instance
  return instance


@pytest.fixture(name='mock_get_config')
def fixture_mock_get_config(mocker: mock.Mock) -> mock.Mock:
  """Mocks _get_config_from_sheet."""
  return mocker.patch('main._get_config_from_sheet')


@pytest.fixture(name='mock_get_placement_lists')
def fixture_mock_get_placement_lists(mocker: mock.Mock) -> mock.Mock:
  """Mocks _get_placement_exclusion_lists_for_mcc."""
  return mocker.patch('main._get_placement_exclusion_lists_for_mcc')


@pytest.fixture(name='mock_get_exclusion_contents')
def fixture_mock_get_exclusion_contents(mocker: mock.Mock) -> mock.Mock:
  """Mocks _get_exclusion_list_contents."""
  return mocker.patch('main._get_exclusion_list_contents')


@pytest.fixture(name='mock_write_to_bq')
def fixture_mock_write_to_bq(mocker: mock.Mock) -> mock.Mock:
  """Mocks _write_data_to_bq."""
  return mocker.patch('main._write_data_to_bq')


@pytest.fixture(name='default_run_config')
def fixture_default_run_config() -> list[dict[str, Any]]:
  """Provides a default valid configuration for the 'run' script tests."""
  return [{
      'sheet_id': TEST_SHEET_ID,
      'customer_id': TEST_CUSTOMER_ID_1,
      'mcc_for_exclusions': TEST_MCC_EXCLUSION_ID_1,
      'lookback_days': 90,
      'gads_filters': 'metrics.clicks > 0',
      'settings': {},
  }]


def test_main_function_success(mocker: mock.Mock) -> None:
  """Tests the main function's successful flow up to calling run()."""
  test_sheet_id = 'sheet1234'
  pubsub_payload = {'sheet_id': test_sheet_id}
  mock_event = mock.MagicMock()
  mock_event.data = create_mock_cloud_event_data(pubsub_payload)

  mock_b64decode = mocker.patch(
      'main.base64.b64decode', side_effect=base64.b64decode
  )
  mock_json_loads = mocker.patch('main.json.loads', side_effect=json.loads)
  mock_jsonschema_validate = mocker.patch('main.jsonschema.validate')
  mock_run_logic = mocker.patch('main.run')

  main_function(mock_event)

  mock_b64decode.assert_called_once()
  mock_json_loads.assert_called_once()
  mock_jsonschema_validate.assert_called_once_with(
      instance=pubsub_payload, schema=REQUEST_SCHEMA
  )
  mock_run_logic.assert_called_once_with(sheet_id=test_sheet_id)


def test_main_function_schema_failure(mocker: mock.Mock) -> None:
  """Tests that `main` handles jsonschema.ValidationError correctly."""
  invalid_payload = {'wrong_key': 'sheet123'}
  mock_event = mock.MagicMock()
  mock_event.data = create_mock_cloud_event_data(invalid_payload)

  mocker.patch('main.base64.b64decode', side_effect=base64.b64decode)
  mocker.patch('main.json.loads', side_effect=json.loads)
  mock_jsonschema_validate = mocker.patch(
      'main.jsonschema.validate',
      side_effect=main.jsonschema.exceptions.ValidationError(
          'Test schema error'
      ),
  )
  mock_run_logic = mocker.patch('main.run')

  main_function(mock_event)

  mock_jsonschema_validate.assert_called_once()
  mock_run_logic.assert_not_called()


def test_get_config_parses_enabled_customer_ids_and_mccs(
    sheets_api_mock_setup: tuple[mock.Mock, Callable[..., Any]],
) -> None:
  """Tests parsing of enabled customer IDs and their MCCs."""
  test_sheet_id = 'sheet_customer_ids'
  mock_sheet_credentials = mock.MagicMock(name='MockSheetCredentials')
  mock_values_api, create_get_side_effect = sheets_api_mock_setup

  mock_customer_ids_data = {
      'values': [
          ['Customer ID', 'Enabled?', 'MCC For Exclusions'],
          ['111-222-3333', 'Enabled', 'mcc123'],
          ['444-555-6666', 'Disabled', 'mcc123'],
          ['777-888-9999', 'Enabled', 'mcc456'],
          ['000-000-0000', 'Enabled', ''],
      ]
  }
  sheet_data_map = {'google_ads_customer_ids': mock_customer_ids_data}
  mock_values_api.get.side_effect = create_get_side_effect(sheet_data_map)

  account_configs = _get_config_from_sheet(
      test_sheet_id, mock_sheet_credentials
  )

  assert len(account_configs) == 3
  assert account_configs[0]['customer_id'] == '1112223333'
  assert account_configs[0]['mcc_for_exclusions'] == 'mcc123'
  assert account_configs[0]['lookback_days'] == 1
  assert account_configs[1]['customer_id'] == '7778889999'
  assert account_configs[1]['mcc_for_exclusions'] == 'mcc456'
  assert account_configs[2]['customer_id'] == '0000000000'
  assert not account_configs[2]['mcc_for_exclusions']


def test_get_config_constructs_gads_filters_query(
    sheets_api_mock_setup: tuple[mock.Mock, Callable[..., Any]],
) -> None:
  """Tests construction of the Google Ads filters query string."""
  test_sheet_id = 'sheet_filters'
  mock_sheet_credentials = mock.MagicMock(name='MockSheetCredentials')
  mock_values_api, create_get_side_effect = sheets_api_mock_setup

  mock_gads_filters_data = {
      'values': [
          ['clicks', '>', '10'],
          ['impressions', '<', '1000'],
          ['cost_micros', '=', '50000'],
      ]
  }
  mock_customer_ids_data = {'values': [['cid', 'Enabled', 'mcc']]}
  sheet_data_map = {
      'google_ads_filters': mock_gads_filters_data,
      'google_ads_customer_ids': mock_customer_ids_data,
  }
  mock_values_api.get.side_effect = create_get_side_effect(sheet_data_map)

  account_configs = _get_config_from_sheet(
      test_sheet_id, mock_sheet_credentials
  )

  assert len(account_configs) == 1
  expected_filter_string = (
      'metrics.clicks > 10 AND metrics.impressions < 1000 AND'
      ' metrics.cost_micros = 50000'
  )
  assert account_configs[0]['gads_filters'] == expected_filter_string


def test_get_config_handles_empty_gads_filters(
    sheets_api_mock_setup: tuple[mock.Mock, Callable[..., Any]],
) -> None:
  """Tests graceful handling of empty Google Ads filters."""
  test_sheet_id = 'sheet_no_filters'
  mock_sheet_credentials = mock.MagicMock(name='MockSheetCredentials')
  mock_values_api, create_get_side_effect = sheets_api_mock_setup

  mock_gads_filters_data = {'values': []}
  mock_customer_ids_data = {'values': [['cid', 'Enabled', 'mcc']]}
  sheet_data_map = {
      'google_ads_filters': mock_gads_filters_data,
      'google_ads_customer_ids': mock_customer_ids_data,
  }
  mock_values_api.get.side_effect = create_get_side_effect(sheet_data_map)

  account_configs = _get_config_from_sheet(
      test_sheet_id, mock_sheet_credentials
  )

  assert len(account_configs) == 1
  assert not account_configs[0]['gads_filters']


def test_get_config_parses_lookback_days(
    sheets_api_mock_setup: tuple[mock.Mock, Callable[..., Any]],
) -> None:
  """Tests parsing of lookback_days."""
  test_sheet_id = 'sheet_lookback'
  mock_sheet_credentials = mock.MagicMock(name='MockSheetCredentials')
  mock_values_api, create_get_side_effect = sheets_api_mock_setup

  mock_customer_ids_data = {'values': [['cid', 'Enabled', 'mcc']]}
  mock_lookback_days_data = {'values': [['90']]}
  sheet_data_map = {
      'google_ads_lookback_days': mock_lookback_days_data,
      'google_ads_customer_ids': mock_customer_ids_data,
  }
  mock_values_api.get.side_effect = create_get_side_effect(sheet_data_map)

  configs_provided = _get_config_from_sheet(
      test_sheet_id, mock_sheet_credentials
  )

  assert len(configs_provided) == 1
  assert configs_provided[0]['lookback_days'] == 90


def test_get_config_returns_default_lookback_days(
    sheets_api_mock_setup: tuple[mock.Mock, Callable[..., Any]],
) -> None:
  """Tests returning the default value for lookback_days when not specified."""
  test_sheet_id = 'sheet_default_lookback'
  mock_sheet_credentials = mock.MagicMock(name='MockSheetCredentials')
  mock_values_api, create_get_side_effect = sheets_api_mock_setup

  mock_customer_ids_data = {'values': [['cid', 'Enabled', 'mcc']]}
  sheet_data_map = {'google_ads_customer_ids': mock_customer_ids_data}
  mock_values_api.get.side_effect = create_get_side_effect(sheet_data_map)

  configs_default = _get_config_from_sheet(
      test_sheet_id, mock_sheet_credentials
  )

  assert len(configs_default) == 1
  assert configs_default[0]['lookback_days'] == 1


def test_get_config_parses_settings_dictionary(
    sheets_api_mock_setup: tuple[mock.Mock, Callable[..., Any]],
) -> None:
  """Tests parsing of the settings into a dictionary."""
  test_sheet_id = 'sheet_settings'
  mock_sheet_credentials = mock.MagicMock(name='MockSheetCredentials')
  mock_values_api, create_get_side_effect = sheets_api_mock_setup

  mock_settings_data = {
      'values': [
          ['foo_setting', 'bar_value'],
          ['another_setting', 'true'],
          ['setting_with_one_val_should_be_ignored'],
          [],
      ]
  }
  mock_customer_ids_data = {'values': [['cid', 'Enabled', 'mcc']]}
  sheet_data_map = {
      'configuration': mock_settings_data,
      'google_ads_customer_ids': mock_customer_ids_data,
  }
  mock_values_api.get.side_effect = create_get_side_effect(sheet_data_map)

  account_configs = _get_config_from_sheet(
      test_sheet_id, mock_sheet_credentials
  )

  assert len(account_configs) == 1
  assert account_configs[0]['settings'] == {
      'foo_setting': 'bar_value',
      'another_setting': 'true',
  }
  assert account_configs[0]['settings'] == {
      'foo_setting': 'bar_value',
      'another_setting': 'true',
  }


def test_run_no_config_from_sheet(
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    mock_gads_client_init: mock.Mock,
    mock_write_to_bq: mock.Mock,
) -> None:
  """Tests run() when _get_config_from_sheet returns an empty list."""
  mock_get_config.return_value = []

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  mock_gads_client_init.assert_not_called()
  mock_write_to_bq.assert_not_called()


def test_run_ads_client_initialization_failure(
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_gads_client_init: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_write_to_bq: mock.Mock,
) -> None:
  """Tests run() when GoogleAdsClient initialization fails."""
  mock_get_config.return_value = default_run_config
  mock_gads_client_init.side_effect = gads_errors.GoogleAdsException(
      failure=None, error=None, request_id=None, call=mock.MagicMock()
  )

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  mock_gads_client_init.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_not_called()
  mock_write_to_bq.assert_not_called()


def test_run_no_mccs_for_exclusions(
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_gads_client_init: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_write_to_bq: mock.Mock,
) -> None:
  """Tests run() when config is valid but contains no MCCs for exclusions."""
  config_no_mccs = [{
      **default_run_config[0],
      'mcc_for_exclusions': '',
  }]
  mock_get_config.return_value = config_no_mccs

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  mock_gads_client_init.assert_not_called()
  mock_get_placement_lists.assert_not_called()
  mock_write_to_bq.assert_not_called()


def test_run_no_exclusion_lists_found_for_mcc(
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_ads_client_instance: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_get_exclusion_contents: mock.Mock,
    mock_write_to_bq: mock.Mock,
):
  """Tests run() when no exclusion lists are found for any MCC."""
  mock_get_config.return_value = default_run_config
  mock_get_placement_lists.return_value = {}

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  main.gads_client.GoogleAdsClient.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
  )
  mock_get_exclusion_contents.assert_not_called()
  mock_write_to_bq.assert_not_called()


def test_run_no_placements_in_any_list(
    mocker: mock.Mock,
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_ads_client_instance: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_get_exclusion_contents: mock.Mock,
    mock_write_to_bq: mock.Mock,
):
  """Tests run() when exclusion lists are found but they have no placements."""
  mock_get_config.return_value = default_run_config
  mock_found_lists_data = [
      {
          'name': EXPECTED_LIST_NAMES[0].format(
              exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1
          ),
          'resource_name': 'sharedSets/111',
          'exclusion_list_mcc_id': TEST_MCC_EXCLUSION_ID_1,
      }
  ]
  mock_get_placement_lists.return_value = mock_found_lists_data
  mock_get_exclusion_contents.return_value = []

  mock_datetime = mocker.patch('main.datetime')
  fixed_now = datetime.datetime(2025, 1, 1, 12, 0, 0)
  mock_datetime.datetime.now.return_value = fixed_now

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  main.gads_client.GoogleAdsClient.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
  )
  mock_get_exclusion_contents.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
      exclusion_list_resource_name='sharedSets/111',
      exclusion_list_name=mock_found_lists_data[0]['name'],
  )
  mock_write_to_bq.assert_not_called()


def test_run_successful_flow_with_data(
    mocker: mock.Mock,
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_ads_client_instance: mock.Mock,
    mock_bq_client_init: mock.Mock,
    mock_bq_client_instance: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_get_exclusion_contents: mock.Mock,
    mock_write_to_bq: mock.Mock,
):
  """Tests a successful run() flow where data is fetched and written to BQ."""
  mock_get_config.return_value = default_run_config
  mock_found_lists_data = [
      {
          'name': EXPECTED_LIST_NAMES[0].format(
              exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
          ),
          'resource_name': 'customers/123/sharedSets/111',
          'exclusion_list_mcc_id': TEST_MCC_EXCLUSION_ID_1,
      }
  ]
  mock_get_placement_lists.return_value = mock_found_lists_data

  mock_datetime = mocker.patch('main.datetime')
  fixed_now = datetime.datetime(2025, 1, 1, 12, 0, 0)
  mock_datetime.datetime.now.return_value = fixed_now
  expected_timestamp = fixed_now.strftime('%Y-%m-%d %H:%M:%S')

  mock_placements_data = [{
      'id': 'video123',
      'customer_id': TEST_MCC_EXCLUSION_ID_1,
      'exclusion_list': mock_found_lists_data[0]['name'],
      'exclusion_type': 'YOUTUBE_VIDEO',
      'exclusion_resource_name': (
          'customers/123/sharedCriteria/video123_criterion'
      ),
      'datetime_updated': expected_timestamp,
  }]
  mock_get_exclusion_contents.return_value = mock_placements_data

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  main.gads_client.GoogleAdsClient.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
  )
  mock_get_exclusion_contents.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
      exclusion_list_resource_name='customers/123/sharedSets/111',
      exclusion_list_name=mock_found_lists_data[0]['name'],
  )
  mock_bq_client_init.assert_called_once_with()
  mock_write_to_bq.assert_called_once_with(
      client=mock_bq_client_instance,
      data_list=mock_placements_data,
      table_name=TEST_TABLE_ID,
      project_id=TEST_PROJECT_ID,
      dataset_id=TEST_DATASET_ID,
  )


def test_run_get_placement_lists_returns_empty_dict_handled(
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_ads_client_instance: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_get_exclusion_contents: mock.Mock,
    mock_write_to_bq: mock.Mock,
):
  """Tests run() correctly handles when _get_placement_exclusion_lists_for_mcc returns empty (error handled within)."""
  mock_get_config.return_value = default_run_config
  mock_get_placement_lists.return_value = {}

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  main.gads_client.GoogleAdsClient.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
  )
  mock_get_exclusion_contents.assert_not_called()
  mock_write_to_bq.assert_not_called()


def test_run_get_exclusion_contents_returns_empty_list_handled(
    mocker: mock.Mock,
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_ads_client_instance: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_get_exclusion_contents: mock.Mock,
    mock_write_to_bq: mock.Mock,
) -> None:
  """Tests run() correctly handles when _get_exclusion_list_contents returns empty (error handled within)."""
  mock_get_config.return_value = default_run_config
  mock_found_lists_data = [
      {
          'name': EXPECTED_LIST_NAMES[0].format(
              exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1
          ),
          'resource_name': 'customers/123/sharedSets/111',
          'exclusion_list_mcc_id': TEST_MCC_EXCLUSION_ID_1,
      }
  ]
  mock_get_placement_lists.return_value = mock_found_lists_data
  mock_get_exclusion_contents.return_value = []

  mock_datetime = mocker.patch('main.datetime')
  fixed_now = datetime.datetime(2025, 1, 1, 12, 0, 0)
  mock_datetime.datetime.now.return_value = fixed_now

  run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  main.gads_client.GoogleAdsClient.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
  )
  mock_get_exclusion_contents.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
      exclusion_list_resource_name='customers/123/sharedSets/111',
      exclusion_list_name=mock_found_lists_data[0]['name'],
  )
  mock_write_to_bq.assert_not_called()


def test_run_write_to_bq_api_error(
    mocker: mock.Mock,
    mock_credentials: mock.Mock,
    mock_get_config: mock.Mock,
    default_run_config: list[dict[str, Any]],
    mock_ads_client_instance: mock.Mock,
    mock_bq_client_init: mock.Mock,
    mock_bq_client_instance: mock.Mock,
    mock_get_placement_lists: mock.Mock,
    mock_get_exclusion_contents: mock.Mock,
    mock_write_to_bq: mock.Mock,
) -> None:
  """Tests run() when _write_data_to_bq raises an API error that run() should propagate."""
  mock_get_config.return_value = default_run_config
  mock_found_lists_data = [
      {
          'name': EXPECTED_LIST_NAMES[0].format(
              exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1
          ),
          'resource_name': 'customers/123/sharedSets/111',
          'exclusion_list_mcc_id': TEST_MCC_EXCLUSION_ID_1,
      }
  ]
  mock_get_placement_lists.return_value = mock_found_lists_data

  mock_datetime = mocker.patch('main.datetime')
  fixed_now = datetime.datetime(2025, 1, 1, 12, 0, 0)
  mock_datetime.datetime.now.return_value = fixed_now
  expected_timestamp = fixed_now.strftime('%Y-%m-%d %H:%M:%S')

  mock_placements_data = [{
      'id': 'video123',
      'customer_id': TEST_MCC_EXCLUSION_ID_1,
      'exclusion_list': mock_found_lists_data[0]['name'],
      'exclusion_type': 'YOUTUBE_VIDEO',
      'exclusion_resource_name': (
          'customers/123/sharedCriteria/video123_criterion'
      ),
      'datetime_updated': expected_timestamp,
  }]
  mock_get_exclusion_contents.return_value = mock_placements_data
  mock_write_to_bq.side_effect = api_exceptions.GoogleAPIError(
      'Simulated BigQuery write failed'
  )

  with pytest.raises(
      api_exceptions.GoogleAPIError, match='Simulated BigQuery write failed'
  ):
    run_script(sheet_id=TEST_SHEET_ID)

  main.google.auth.default.assert_called_once_with(scopes=main.SCOPES)
  mock_get_config.assert_called_once_with(
      sheet_id=TEST_SHEET_ID, credentials=mock_credentials
  )
  main.gads_client.GoogleAdsClient.assert_called_once_with(
      version=TEST_CLIENT_VERSION,
      credentials=mock_credentials,
      developer_token=TEST_DEV_TOKEN,
      login_customer_id=TEST_LOGIN_CUSTOMER_ID,
      use_proto_plus=os.environ.get('GOOGLE_ADS_USE_PROTO_PLUS'),
  )
  mock_get_placement_lists.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
  )
  mock_get_exclusion_contents.assert_called_once_with(
      client=mock_ads_client_instance,
      exclusion_list_mcc_id=TEST_MCC_EXCLUSION_ID_1,
      exclusion_list_resource_name='customers/123/sharedSets/111',
      exclusion_list_name=mock_found_lists_data[0]['name'],
  )
  mock_bq_client_init.assert_called_once_with()
  mock_write_to_bq.assert_called_once_with(
      client=mock_bq_client_instance,
      data_list=mock_placements_data,
      table_name=TEST_TABLE_ID,
      project_id=TEST_PROJECT_ID,
      dataset_id=TEST_DATASET_ID,
  )
