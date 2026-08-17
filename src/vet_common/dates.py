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

"""Date and lookback calculation utilities for reporting queries."""

import datetime
from typing import Optional


def get_lookback_date_range(
    lookback_days: int,
    today: Optional[datetime.date] = None,
    dt_format: str = '%Y-%m-%d',
) -> tuple[str, str]:
  """Returns a tuple of formatted string dates for lookback reporting queries.

  Calculates the start date by subtracting lookback_days from today, returning
  both start and end dates as strings in the requested format.

  Args:
      lookback_days: The number of days from today to look back.
      today: The reference date representing today (defaults to
        datetime.date.today()).
      dt_format: Date format string (defaults to '%Y-%m-%d').

  Returns:
      A tuple containing (date_from, date_to) formatted strings.
  """
  if today is None:
    today = datetime.date.today()
  date_from = today - datetime.timedelta(days=lookback_days)
  return (
      date_from.strftime(dt_format),
      today.strftime(dt_format),
  )
