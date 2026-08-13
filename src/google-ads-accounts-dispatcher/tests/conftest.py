"""Pytest configuration for the google-ads-accounts-dispatcher module."""

import os
import sys

TEST_SHEET_ID = 'test_sheet_123'
TEST_PROJECT_ID = 'test-gcp-project'
TEST_TOPIC_ID = 'test-topic'

project_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root_dir)
src_dir = os.path.dirname(project_root_dir)
if src_dir not in sys.path:
  sys.path.insert(0, src_dir)
print(f'\n[conftest.py] Added to sys.path: {project_root_dir}, {src_dir}')

_original_env_vars = {}
_env_vars_to_manage = [
    'IS_LOCAL_TEST',
    'GOOGLE_CLOUD_PROJECT',
    'VET_GOOGLE_ADS_ACCOUNT_TOPIC',
    'K_SERVICE',
]

TEST_SESSION_ENV_VALUES = {
    'IS_LOCAL_TEST': 'True',
    'GOOGLE_CLOUD_PROJECT': TEST_PROJECT_ID,
    'VET_GOOGLE_ADS_ACCOUNT_TOPIC': TEST_TOPIC_ID,
    'K_SERVICE': 'test-gads-account-dispatcher',
}


def pytest_configure(config):
  """Sets up the environment for testing."""
  del config  # Unused
  print(
      '[conftest.py:pytest_configure] Setting environment variables for'
      ' google-ads-accounts-dispatcher test session.'
  )
  for key in _env_vars_to_manage:
    _original_env_vars[key] = os.environ.get(key)
    if key in TEST_SESSION_ENV_VALUES:
      os.environ[key] = TEST_SESSION_ENV_VALUES[key]
      print(f"  Set: {key} = '{TEST_SESSION_ENV_VALUES[key]}'")


def pytest_unconfigure(config):
  """Cleans up the environment after testing."""
  del config  # Unused
  print(
      '[conftest.py:pytest_unconfigure] Restoring original environment'
      ' variables.'
  )
  for key in _env_vars_to_manage:
    original_value = _original_env_vars.get(key)
    if original_value is None:
      if key in os.environ:
        del os.environ[key]
        print(f'  Removed: {key}')
    else:
      os.environ[key] = original_value
      print(f'  Restored: {key} = "{original_value}"')
  _original_env_vars.clear()
