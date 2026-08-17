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

"""Shared Google Ads API helpers and version configuration."""

import os

DEFAULT_GOOGLE_ADS_API_VERSION = 'v25'


def get_google_ads_client_version() -> str:
  """Returns the configured or default Google Ads API version string."""
  return os.environ.get(
      'GOOGLE_ADS_CLIENT_VERSION', DEFAULT_GOOGLE_ADS_API_VERSION
  )
