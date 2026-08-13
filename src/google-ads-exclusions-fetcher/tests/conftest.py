"""Pytest configuration for the google-ads-exclusions-fetcher module."""
import os
import sys

TEST_SHEET_ID = 'test_sheet_123'
TEST_PROJECT_ID = 'test-gcp-project'
TEST_DATASET_ID = 'test_dataset'
TEST_TABLE_ID = 'test_table'
TEST_LOGIN_CUSTOMER_ID = '1112223333'
TEST_DEV_TOKEN = 'test_developer_token'
TEST_CLIENT_VERSION = 'v17'
TEST_MCC_EXCLUSION_ID_1 = '5556667777'
TEST_CUSTOMER_ID_1 = '1234567890'

project_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root_dir)
print(f'\n[conftest.py] Added to sys.path: {project_root_dir}')

_original_env_vars = {}
_env_vars_to_manage = [
    'IS_LOCAL_TEST',
    'GOOGLE_CLOUD_PROJECT',
    'GOOGLE_ADS_CLIENT_VERSION',
    'GOOGLE_ADS_LOGIN_CUSTOMER_ID',
    'GOOGLE_ADS_DEVELOPER_TOKEN',
    'GOOGLE_ADS_USE_PROTO_PLUS',
    'VET_BIGQUERY_TARGET_DATASET',
    'VET_BIGQUERY_TARGET_TABLE',
]

# Values to set for the test session, using the constants defined above
TEST_SESSION_ENV_VALUES = {
    'IS_LOCAL_TEST': 'True',
    'GOOGLE_CLOUD_PROJECT': TEST_PROJECT_ID,
    'GOOGLE_ADS_CLIENT_VERSION': TEST_CLIENT_VERSION,
    'GOOGLE_ADS_LOGIN_CUSTOMER_ID': TEST_LOGIN_CUSTOMER_ID,
    'GOOGLE_ADS_DEVELOPER_TOKEN': TEST_DEV_TOKEN,
    'GOOGLE_ADS_USE_PROTO_PLUS': 'True',
    'VET_BIGQUERY_TARGET_DATASET': TEST_DATASET_ID,
    'VET_BIGQUERY_TARGET_TABLE': TEST_TABLE_ID,
}


def pytest_configure(config):
  """Sets up the environment for testing.

  Args:
    config: The pytest configuration object.
  """
  del config  # Unused
  print(
      '[conftest.py:pytest_configure] Setting environment variables for test'
      ' session.'
  )
  for key in _env_vars_to_manage:
    _original_env_vars[key] = os.environ.get(key)  # Store original
    if key in TEST_SESSION_ENV_VALUES:
      os.environ[key] = TEST_SESSION_ENV_VALUES[key]  # Set test value
      print(f"  Set: {key} = '{TEST_SESSION_ENV_VALUES[key]}'")
    elif (
        key == 'IS_LOCAL_TEST' and key not in TEST_SESSION_ENV_VALUES
    ):  # Fallback for IS_LOCAL_TEST if not in map
      os.environ['IS_LOCAL_TEST'] = 'True'
      print(f"  Set: {key} = 'True' (fallback)")


def pytest_unconfigure(config):
  """Cleans up the environment after testing.

  Args:
    config: The pytest configuration object.
  """
  del config  # Unused.
  print(
      '[conftest.py:pytest_unconfigure] Restoring original environment'
      ' variables.'
  )
  for key in _env_vars_to_manage:
    original_value = _original_env_vars.get(key)
    if original_value is None:
      if (
          key in os.environ
      ):  # Only delete if it was set by us and not present before
        del os.environ[key]
        print(f'  Removed: {key}')
    else:
      os.environ[key] = original_value
      print(f'  Restored: {key} = "{original_value}"')
  _original_env_vars.clear()
