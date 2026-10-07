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

"""Unit tests for gemini_video_evaluator.utils.config."""

import datetime

from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import sql
import pytest

_NOW = datetime.datetime(2026, 9, 28, 12, 0, tzinfo=datetime.timezone.utc)
_CEILING = 500


def _settings(**values):
  """Builds gemini_settings rows, one per keyword argument."""
  return [{'setting': name, 'value': value} for name, value in values.items()]


def _category(
    category, mode='Shadow', min_score='0.70', description='A description.'
):
  """Builds one gemini_categories row."""
  return {
      'category': category,
      'mode': mode,
      'min_score': min_score,
      'description': description,
  }


def _parse(settings_rows=(), category_rows=()):
  return config_lib.parse_sheet_config(
      settings_rows, category_rows, max_videos_ceiling=_CEILING, now=_NOW
  )


def _problems(settings_rows=(), category_rows=()):
  with pytest.raises(config_lib.SheetConfigError) as exc_info:
    _parse(settings_rows, category_rows)
  return exc_info.value.problems


def test_empty_tabs_take_the_defaults_and_ask_nothing():
  """Verifies that a Sheet with nothing filled in is valid and inactive."""
  sheet_config, warnings = _parse()

  assert not warnings
  assert sheet_config.age_check_mode == config_lib.MODE_DISABLED
  assert sheet_config.max_minor_age == config_lib.DEFAULT_MAX_MINOR_AGE
  assert not sheet_config.advertiser_context
  assert sheet_config.max_videos_per_batch == _CEILING
  assert sheet_config.reevaluate_before is None
  assert not sheet_config.categories
  assert not sheet_config.anything_active


def test_a_complete_sheet_is_parsed():
  """Verifies every setting and category column is read."""
  sheet_config, warnings = _parse(
      _settings(
          age_check_mode='Exclude',
          max_minor_age='15',
          advertiser_context='  A spirits brand.  ',
          max_videos_per_batch='200',
          reevaluate_before='2026-09-01 06:30',
      ),
      [
          _category('violence_gore', 'Exclude', '0.8', 'Graphic violence.'),
          _category('political_extremism', 'Shadow', '', 'Extremism.'),
          _category('profanity', 'Disabled', '', ''),
      ],
  )

  assert not warnings
  assert sheet_config.age_check_mode == config_lib.MODE_EXCLUDE
  assert sheet_config.age_check_active
  assert sheet_config.max_minor_age == 15
  assert sheet_config.advertiser_context == 'A spirits brand.'
  assert sheet_config.max_videos_per_batch == 200
  assert sheet_config.reevaluate_before == datetime.datetime(
      2026, 9, 1, 6, 30, tzinfo=datetime.timezone.utc
  )
  assert sheet_config.categories == (
      config_lib.Category(
          'violence_gore', config_lib.MODE_EXCLUDE, 0.8, 'Graphic violence.'
      ),
      config_lib.Category(
          'political_extremism',
          config_lib.MODE_SHADOW,
          config_lib.DEFAULT_MIN_SCORE,
          'Extremism.',
      ),
      config_lib.Category(
          'profanity', config_lib.MODE_DISABLED, config_lib.DEFAULT_MIN_SCORE, ''
      ),
  )
  assert [c.name for c in sheet_config.active_categories] == [
      'violence_gore',
      'political_extremism',
  ]


def test_values_are_read_the_way_people_type_them():
  """Verifies case-insensitive names and modes, percentages and '17.0'."""
  sheet_config, _ = _parse(
      _settings(AGE_CHECK_MODE='shadow', max_minor_age='17.0'),
      [_category('Kids', 'EXCLUDE', '70%')],
  )

  assert sheet_config.age_check_mode == config_lib.MODE_SHADOW
  assert sheet_config.max_minor_age == 17
  assert sheet_config.categories == (
      config_lib.Category(
          'kids', config_lib.MODE_EXCLUDE, 0.7, 'A description.'
      ),
  )


def test_blank_cells_take_the_defaults():
  """Verifies a blank mode means Disabled and a blank min_score 0.70."""
  sheet_config, _ = _parse(
      category_rows=[
          _category('gambling', mode='', min_score='', description='')
      ]
  )

  (category,) = sheet_config.categories
  assert category.mode == config_lib.MODE_DISABLED
  assert category.min_score == config_lib.DEFAULT_MIN_SCORE
  assert not category.active


def test_blank_rows_are_ignored():
  """Verifies that empty rows in either tab are skipped without a problem."""
  sheet_config, _ = _parse(
      [{'setting': None, 'value': None}, {'setting': ' ', 'value': ''}],
      [{'category': None, 'mode': None, 'min_score': None, 'description': ''}],
  )

  assert not sheet_config.categories


def test_min_score_is_rounded_to_the_stored_precision():
  """Verifies min_score keeps the 4 decimal places detection scores have."""
  sheet_config, _ = _parse(
      category_rows=[_category('kids', min_score='70.12345%')]
  )

  assert sheet_config.categories[0].min_score == 0.7012


def test_max_videos_above_the_ceiling_is_capped_with_a_warning():
  """Verifies the Sheet cannot raise the batch size set at deployment."""
  sheet_config, warnings = _parse(_settings(max_videos_per_batch='5000'))

  assert sheet_config.max_videos_per_batch == _CEILING
  assert len(warnings) == 1
  assert 'max_videos_per_batch is 5000' in warnings[0]
  assert f'so {_CEILING} is used' in warnings[0]


def test_max_videos_of_zero_pauses_sending():
  """Verifies 0 is accepted, so that sending can be paused from the Sheet."""
  sheet_config, warnings = _parse(_settings(max_videos_per_batch='0'))

  assert sheet_config.max_videos_per_batch == 0
  assert not warnings


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        (
            '2026-09-01',
            datetime.datetime(2026, 9, 1, tzinfo=datetime.timezone.utc),
        ),
        (
            '2026-09-01T10:00:00+02:00',
            datetime.datetime(2026, 9, 1, 8, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            '2026-09-01T08:00:00Z',
            datetime.datetime(2026, 9, 1, 8, 0, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_reevaluate_before_is_read_as_utc(raw, expected):
  """Verifies dates, offsets and Z are all converted to UTC."""
  sheet_config, _ = _parse(_settings(reevaluate_before=raw))

  assert sheet_config.reevaluate_before == expected


@pytest.mark.parametrize(
    ('settings_rows', 'category_rows', 'expected'),
    [
        (
            _settings(age_check_mode='On'),
            [],
            "age_check_mode is 'On'. Use Disabled, Shadow or Exclude.",
        ),
        (_settings(max_minor_age='170'), [], "max_minor_age is '170'"),
        (_settings(max_minor_age='16.5'), [], 'whole number from 1 to 30'),
        (_settings(max_minor_age='0'), [], 'whole number from 1 to 30'),
        (_settings(max_videos_per_batch='-1'), [], '0 pauses sending'),
        (_settings(max_videos_per_batch='lots'), [], '0 pauses sending'),
        (
            _settings(reevaluate_before='01/09/2026'),
            [],
            'format the cell as Plain text',
        ),
        (_settings(reevaluate_before='2026-09-29'), [], 'in the future'),
        (
            [{'setting': 'max_video', 'value': '5'}],
            [],
            "'max_video' is not a known setting",
        ),
        (
            [{'setting': '', 'value': '5'}],
            [],
            "the value '5' has no setting name",
        ),
        (
            [
                {'setting': 'max_minor_age', 'value': '15'},
                {'setting': 'MAX_MINOR_AGE', 'value': '16'},
            ],
            [],
            'max_minor_age appears more than once',
        ),
        (
            [],
            [_category('kids', min_score='0.07')],
            'Use a number between 0.50 and 1.00, or 50% to 100%.',
        ),
        ([], [_category('kids', min_score='150%')], 'min_score of kids'),
        ([], [_category('kids', min_score='high')], 'min_score of kids'),
        ([], [_category('kids', mode='Block')], "the mode of kids is 'Block'"),
        ([], [_category('kids-content')], "'kids-content' is not valid"),
        ([], [_category('k')], "'k' is not valid"),
        ([], [_category('evaluated')], 'is used by the tool itself'),
        ([], [_category('kids', description='')], 'kids has no description'),
        (
            [],
            [_category('', description='x')],
            'no category name in column A',
        ),
        (
            [],
            [_category('kids'), _category('kids')],
            'the category kids appears more than once',
        ),
    ],
)
def test_each_problem_is_explained(settings_rows, category_rows, expected):
  """Verifies each kind of bad value produces one plain-language problem."""
  problems = _problems(settings_rows, category_rows)

  assert len(problems) == 1
  assert expected in problems[0]


def test_a_name_that_starts_another_is_accepted():
  """Verifies kids and kids_gaming can both be used.

  The exclusion views match a Sheet policy to its own taxonomy only, so the
  kids rule never judges kids_gaming scores.
  """
  sheet_config, _ = _parse(
      category_rows=[_category('kids'), _category('kids_gaming')]
  )

  assert [c.name for c in sheet_config.categories] == ['kids', 'kids_gaming']


def test_every_problem_is_reported_at_once():
  """Verifies the error lists all problems under a headline counting them."""
  with pytest.raises(config_lib.SheetConfigError) as exc_info:
    _parse(
        _settings(age_check_mode='On', max_minor_age='abc'),
        [_category('kids', min_score='0.07'), _category('evaluated')],
    )

  error = exc_info.value
  assert len(error.problems) == 4
  message = str(error)
  assert message.startswith(
      'The Config Sheet has 4 problems. Until they are fixed, no new videos'
  )
  for problem in error.problems:
    assert f'\n- {problem}' in message


def test_a_single_problem_has_a_singular_headline():
  """Verifies the headline reads naturally for one problem."""
  with pytest.raises(config_lib.SheetConfigError) as exc_info:
    _parse(_settings(age_check_mode='On'))

  assert str(exc_info.value).startswith(
      'The Config Sheet has 1 problem. Until it is fixed, no new videos'
  )


def test_the_label_of_the_evaluated_marker_is_reserved():
  """Verifies a category cannot share the audit row's key in detection."""
  assert sql.LABEL_EVALUATED in config_lib.RESERVED_CATEGORY_NAMES
