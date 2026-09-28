"""Pytest configuration for the keyword_matcher module."""

import os
import sys

TEST_PROJECT_ID = 'test-gcp-project'
TEST_DATASET_ID = 'test_dataset'

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
    'VET_BIGQUERY_DATASET',
    'VET_VIDEO_CORPUS_TABLE',
    'VET_CHANNEL_CORPUS_TABLE',
    'VET_KEYWORD_TABLE',
    'VET_DETECTION_TABLE',
    'VET_MIN_RETENTION_RATIO',
    'VET_MIN_KEYWORD_RETENTION_RATIO',
    'K_SERVICE',
]

TEST_SESSION_ENV_VALUES = {
    # No longer load-bearing: main.py uses the shared stdout logger, so nothing
    # constructs a Cloud Logging client on import. Kept, and kept set, only to
    # match the conftest shape used by the other services.
    'IS_LOCAL_TEST': 'True',
    'GOOGLE_CLOUD_PROJECT': TEST_PROJECT_ID,
    'VET_BIGQUERY_DATASET': TEST_DATASET_ID,
    'VET_VIDEO_CORPUS_TABLE': 'youtube_video',
    'VET_CHANNEL_CORPUS_TABLE': 'youtube_channel',
    'VET_KEYWORD_TABLE': 'exclusion_keywords',
    'VET_DETECTION_TABLE': 'detection',
    'VET_MIN_RETENTION_RATIO': '0.9',
    'VET_MIN_KEYWORD_RETENTION_RATIO': '0.9',
    'K_SERVICE': 'test-keyword-matcher',
}


def pytest_configure(config):
  """Sets up the environment for testing."""
  del config  # Unused
  print(
      '[conftest.py:pytest_configure] Setting environment variables for'
      ' keyword_matcher test session.'
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
