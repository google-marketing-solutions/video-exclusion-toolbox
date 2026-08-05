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

"""ID sanitization utilities for Google Ads Customer and MCC IDs."""

from typing import Any


def sanitize_gads_id(raw_id: Any) -> str:
  """Strips hyphens and whitespace from a Google Ads ID to return a clean numerical string.

  Args:
      raw_id: The ID string or integer from a spreadsheet or API response.

  Returns:
      A sanitized ID string without hyphens or leading/trailing whitespace.
  """
  return str(raw_id).replace('-', '').strip()
