"""Pytest configuration for the gads_video_report_fetcher module."""

import os
import sys

TEST_PROJECT_ID = 'test-gcp-project'
TEST_DATASET_ID = 'test_dataset'
TEST_TOPIC_ID = 'test-yt-video-topic'

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
    'VID_EXCL_BIGQUERY_DATASET',
    'VID_EXCL_YOUTUBE_VIDEO_PUBSUB_TOPIC',
    'GOOGLE_ADS_CLIENT_VERSION',
    'GOOGLE_ADS_DEVELOPER_TOKEN',
    'GOOGLE_ADS_LOGIN_CUSTOMER_ID',
    'GOOGLE_ADS_USE_PROTO_PLUS',
    'K_SERVICE',
]

TEST_SESSION_ENV_VALUES = {
    'IS_LOCAL_TEST': 'True',
    'GOOGLE_CLOUD_PROJECT': TEST_PROJECT_ID,
    'VID_EXCL_BIGQUERY_DATASET': TEST_DATASET_ID,
    'VID_EXCL_YOUTUBE_VIDEO_PUBSUB_TOPIC': TEST_TOPIC_ID,
    'GOOGLE_ADS_CLIENT_VERSION': 'v25',
    'GOOGLE_ADS_DEVELOPER_TOKEN': 'test_developer_token',
    'GOOGLE_ADS_LOGIN_CUSTOMER_ID': '1234567890',
    'GOOGLE_ADS_USE_PROTO_PLUS': 'false',
    'K_SERVICE': 'test-gads-video-report-fetcher',
}


def pytest_configure(config):
  """Sets up the environment for testing."""
  del config  # Unused
  print(
      '[conftest.py:pytest_configure] Setting environment variables for'
      ' gads_video_report_fetcher test session.'
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
