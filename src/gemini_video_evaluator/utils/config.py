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

"""Reads the Gemini evaluator settings from the Config Sheet.

The Sheet has two tabs, each exposed to BigQuery as an external table:

  gemini_settings    One row per setting: setting | value.
  gemini_categories  One row per content category:
                     category | mode | min_score | description.

This module is pure. It turns the rows read from those tables into a validated
`SheetConfig`, or raises `SheetConfigError` listing every problem at once. The
messages are written for the person who edits the Sheet, not for a developer.
"""

import dataclasses
import datetime
import math
import re
from typing import Any, Iterable, Mapping, Optional

SETTINGS_TAB = 'gemini_settings'
CATEGORIES_TAB = 'gemini_categories'

MODE_DISABLED = 'Disabled'
MODE_SHADOW = 'Shadow'
MODE_EXCLUDE = 'Exclude'
_MODES = {m.lower(): m for m in (MODE_DISABLED, MODE_SHADOW, MODE_EXCLUDE)}

SETTING_NAMES = (
    'age_check_mode',
    'max_minor_age',
    'advertiser_context',
    'max_videos_per_batch',
    'reevaluate_before',
)

DEFAULT_MAX_MINOR_AGE = 18
# Wide enough for rules phrased as "appears to be under 25", narrow enough to
# catch a typo such as 170.
MAX_MINOR_AGE_RANGE = (1, 30)

DEFAULT_MIN_SCORE = 0.70
# The lower bound is well above zero on purpose: it catches 0.07 typed for
# 0.70, which would otherwise exclude almost every video in the category.
MIN_SCORE_RANGE = (0.50, 1.00)

# Category names become part of the detection taxonomy and of the prompt, so
# they are restricted to short lowercase identifiers.
CATEGORY_NAME_PATTERN = re.compile(r'[a-z][a-z0-9_]{1,63}')
# The row that marks a video as evaluated is labelled 'evaluated', with the
# same modality as the category rows. A category of that name would share its
# key in the detection table, and every merge of its results would fail.
RESERVED_CATEGORY_NAMES = frozenset({'evaluated'})


@dataclasses.dataclass(frozen=True)
class Category:
  """One row of the gemini_categories tab."""

  name: str
  mode: str
  min_score: float
  description: str

  @property
  def active(self) -> bool:
    """Whether Gemini is asked to score this category."""
    return self.mode != MODE_DISABLED


@dataclasses.dataclass(frozen=True)
class SheetConfig:
  """The validated contents of both Gemini tabs of the Config Sheet."""

  age_check_mode: str
  max_minor_age: int
  advertiser_context: str
  max_videos_per_batch: int
  reevaluate_before: Optional[datetime.datetime]
  categories: tuple[Category, ...]

  @property
  def age_check_active(self) -> bool:
    """Whether Gemini is asked to look for minors in the thumbnails."""
    return self.age_check_mode != MODE_DISABLED

  @property
  def active_categories(self) -> tuple[Category, ...]:
    """The categories Gemini is asked to score, in Sheet order."""
    return tuple(c for c in self.categories if c.active)

  @property
  def anything_active(self) -> bool:
    """Whether there is anything to ask Gemini at all."""
    return self.age_check_active or bool(self.active_categories)


class SheetConfigError(ValueError):
  """Raised when the Config Sheet cannot be used as it stands."""

  def __init__(self, problems: Iterable[str]):
    self.problems = tuple(problems)
    count = len(self.problems)
    headline = (
        'The Config Sheet has 1 problem. Until it is fixed'
        if count == 1
        else f'The Config Sheet has {count} problems. Until they are fixed'
    )
    super().__init__(
        f'{headline}, no new videos are sent to Gemini and the exclusion'
        ' rules stay as they are. Results of evaluations that are already'
        ' running are still saved.\n'
        + '\n'.join(f'- {problem}' for problem in self.problems)
    )


def _cell(value: Any) -> str:
  """Returns a Sheet cell as stripped text; an empty cell becomes ''."""
  return '' if value is None else str(value).strip()


def _parse_mode(raw: str, where: str, problems: list[str]) -> str:
  """Parses a mode cell case-insensitively; a blank cell means Disabled."""
  if not raw:
    return MODE_DISABLED
  mode = _MODES.get(raw.lower())
  if mode is None:
    problems.append(f"{where} is '{raw}'. Use Disabled, Shadow or Exclude.")
    return MODE_DISABLED
  return mode


def _parse_whole_number(raw: str) -> Optional[int]:
  """Parses '18', or '18.0' as the Sheet may render it; None otherwise."""
  try:
    number = float(raw)
  except ValueError:
    return None
  return int(number) if math.isfinite(number) and number.is_integer() else None


def _parse_score(raw: str) -> Optional[float]:
  """Parses '0.7' or '70%'; None if the cell is not a number."""
  text, scale = (raw[:-1].strip(), 100.0) if raw.endswith('%') else (raw, 1.0)
  try:
    value = float(text) / scale
  except ValueError:
    return None
  return value if math.isfinite(value) else None


def _parse_timestamp(raw: str) -> Optional[datetime.datetime]:
  """Parses an ISO date or date-time; one without an offset is read as UTC."""
  try:
    parsed = datetime.datetime.fromisoformat(raw)
  except ValueError:
    return None
  if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=datetime.timezone.utc)
  return parsed.astimezone(datetime.timezone.utc)


def _read_settings(
    rows: Iterable[Mapping[str, Any]], problems: list[str]
) -> dict[str, str]:
  """Collects the gemini_settings rows into a name -> raw value mapping."""
  values: dict[str, str] = {}
  for row in rows:
    raw_name = _cell(row.get('setting'))
    value = _cell(row.get('value'))
    if not raw_name:
      if value:
        problems.append(
            f"{SETTINGS_TAB}: the value '{value}' has no setting name next"
            ' to it in column A.'
        )
      continue
    name = raw_name.lower()
    if name not in SETTING_NAMES:
      problems.append(
          f"{SETTINGS_TAB}: '{raw_name}' is not a known setting. The"
          f' settings are: {", ".join(SETTING_NAMES)}.'
      )
    elif name in values:
      problems.append(
          f'{SETTINGS_TAB}: {name} appears more than once. Keep one row for it.'
      )
    else:
      values[name] = value
  return values


def _parse_categories(
    rows: Iterable[Mapping[str, Any]], problems: list[str]
) -> tuple[Category, ...]:
  """Validates the gemini_categories rows."""
  categories: list[Category] = []
  for row in rows:
    raw_name = _cell(row.get('category'))
    raw_mode = _cell(row.get('mode'))
    raw_score = _cell(row.get('min_score'))
    description = _cell(row.get('description'))
    if not (raw_name or raw_mode or raw_score or description):
      continue

    name = raw_name.lower()
    if not raw_name:
      problems.append(
          f'{CATEGORIES_TAB}: a row has something in columns B to D but no'
          ' category name in column A.'
      )
      continue
    if not CATEGORY_NAME_PATTERN.fullmatch(name):
      problems.append(
          f"{CATEGORIES_TAB}: the category name '{raw_name}' is not valid."
          ' Use 2 to 64 lowercase letters, digits and underscores, starting'
          ' with a letter, for example kids_content.'
      )
      continue
    if name in RESERVED_CATEGORY_NAMES:
      problems.append(
          f"{CATEGORIES_TAB}: the category name '{name}' is used by the tool"
          ' itself. Choose another name.'
      )
      continue
    if any(existing.name == name for existing in categories):
      problems.append(
          f'{CATEGORIES_TAB}: the category {name} appears more than once.'
          ' Keep one row for it.'
      )
      continue

    mode = _parse_mode(
        raw_mode, f'{CATEGORIES_TAB}: the mode of {name}', problems
    )

    min_score = DEFAULT_MIN_SCORE
    if raw_score:
      low, high = MIN_SCORE_RANGE
      parsed = _parse_score(raw_score)
      if parsed is None or not low <= parsed <= high:
        problems.append(
            f"{CATEGORIES_TAB}: the min_score of {name} is '{raw_score}'."
            f' Use a number between {low:.2f} and {high:.2f}, or'
            f' {low:.0%} to {high:.0%}.'
        )
      else:
        # Detection scores are stored to 4 decimal places; matching that keeps
        # a threshold such as 70.1% from missing a score of 0.701 by a
        # rounding error.
        min_score = round(parsed, 4)

    if mode != MODE_DISABLED and not description:
      problems.append(
          f'{CATEGORIES_TAB}: {name} has no description. Gemini needs one to'
          ' know what to look for: add it in column D, or set the mode to'
          ' Disabled.'
      )
    categories.append(Category(name, mode, min_score, description))

  return tuple(categories)


def parse_sheet_config(
    settings_rows: Iterable[Mapping[str, Any]],
    category_rows: Iterable[Mapping[str, Any]],
    max_videos_ceiling: int,
    now: datetime.datetime,
) -> tuple[SheetConfig, list[str]]:
  """Validates both Gemini tabs of the Config Sheet."""
  # Missing settings take their defaults: the age check is Disabled, the minor
  # age is 18, and the batch size is the deployment's ceiling.
  problems: list[str] = []
  warnings: list[str] = []
  settings = _read_settings(settings_rows, problems)

  age_check_mode = _parse_mode(
      settings.get('age_check_mode', ''),
      f'{SETTINGS_TAB}: age_check_mode',
      problems,
  )

  max_minor_age = DEFAULT_MAX_MINOR_AGE
  if raw := settings.get('max_minor_age', ''):
    low, high = MAX_MINOR_AGE_RANGE
    parsed_age = _parse_whole_number(raw)
    if parsed_age is None or not low <= parsed_age <= high:
      problems.append(
          f"{SETTINGS_TAB}: max_minor_age is '{raw}'. Use a whole number"
          f' from {low} to {high}, for example 18.'
      )
    else:
      max_minor_age = parsed_age

  max_videos = max_videos_ceiling
  if raw := settings.get('max_videos_per_batch', ''):
    parsed_count = _parse_whole_number(raw)
    if parsed_count is None or parsed_count < 0:
      problems.append(
          f"{SETTINGS_TAB}: max_videos_per_batch is '{raw}'. Use a whole"
          f' number from 0 to {max_videos_ceiling}; 0 pauses sending new'
          ' videos.'
      )
    elif parsed_count > max_videos_ceiling:
      warnings.append(
          f'{SETTINGS_TAB}: max_videos_per_batch is {parsed_count}, above'
          f' the limit of {max_videos_ceiling} set when the tool was'
          f' deployed, so {max_videos_ceiling} is used.'
      )
    else:
      max_videos = parsed_count

  reevaluate_before = None
  if raw := settings.get('reevaluate_before', ''):
    parsed_time = _parse_timestamp(raw)
    if parsed_time is None:
      problems.append(
          f"{SETTINGS_TAB}: reevaluate_before is '{raw}', which is not a"
          ' date in the form 2026-10-01 or 2026-10-01 14:30. If you typed'
          ' it in that form, the Sheet has turned it into a date of its'
          ' own: format the cell as Plain text and type it again.'
      )
    elif parsed_time > now:
      problems.append(
          f"{SETTINGS_TAB}: reevaluate_before is '{raw}', which is in the"
          ' future. The same videos would keep being sent again until that'
          ' time had passed. Use a date and time that has already passed,'
          ' or leave the cell blank.'
      )
    else:
      reevaluate_before = parsed_time

  categories = _parse_categories(category_rows, problems)
  if problems:
    raise SheetConfigError(problems)
  return (
      SheetConfig(
          age_check_mode=age_check_mode,
          max_minor_age=max_minor_age,
          advertiser_context=settings.get('advertiser_context', ''),
          max_videos_per_batch=max_videos,
          reevaluate_before=reevaluate_before,
          categories=categories,
      ),
      warnings,
  )
