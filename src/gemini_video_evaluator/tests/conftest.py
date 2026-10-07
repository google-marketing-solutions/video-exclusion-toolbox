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

"""Pytest configuration for gemini_video_evaluator unit tests."""

import os
import sys

TEST_PROJECT_ID = 'test-gcp-project'
TEST_DATASET_ID = 'test_dataset'

project_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root_dir)
src_dir = os.path.dirname(project_root_dir)
if src_dir not in sys.path:
  sys.path.insert(0, src_dir)

_original_env_vars = {}
_env_vars_to_manage = [
    'IS_LOCAL_TEST',
    'GOOGLE_CLOUD_PROJECT',
    'VET_BIGQUERY_DATASET',
    'K_SERVICE',
]

TEST_SESSION_ENV_VALUES = {
    'IS_LOCAL_TEST': 'True',
    'GOOGLE_CLOUD_PROJECT': TEST_PROJECT_ID,
    'VET_BIGQUERY_DATASET': TEST_DATASET_ID,
    'K_SERVICE': 'test-gemini-video-evaluator',
}


def pytest_configure(config):
  """Sets up environment variables for testing."""
  del config
  for key in _env_vars_to_manage:
    _original_env_vars[key] = os.environ.get(key)
    if key in TEST_SESSION_ENV_VALUES:
      os.environ[key] = TEST_SESSION_ENV_VALUES[key]


def pytest_unconfigure(config):
  """Restores original environment variables after testing."""
  del config
  for key in _env_vars_to_manage:
    original_value = _original_env_vars.get(key)
    if original_value is None:
      os.environ.pop(key, None)
    else:
      os.environ[key] = original_value
  _original_env_vars.clear()
