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

"""Unit tests for gemini_video_evaluator.utils.prompt."""

from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import prompt

_VIDEO = {
    'video_id': 'vid_001',
    'title': 'Unboxing new toys',
    'description': 'We open new toys.',
    'tags': 'toys, unboxing',
}

_CATEGORIES = (
    config_lib.Category('kids', 'Shadow', 0.7, 'Content made for kids.'),
    config_lib.Category('profanity', 'Disabled', 0.7, ''),
    config_lib.Category('violence_gore', 'Exclude', 0.8, 'Graphic violence.'),
)


def _sheet_config(
    age_check_mode='Shadow',
    categories=_CATEGORIES,
    advertiser_context='',
    max_minor_age=18,
):
  return config_lib.SheetConfig(
      age_check_mode=age_check_mode,
      max_minor_age=max_minor_age,
      advertiser_context=advertiser_context,
      max_videos_per_batch=500,
      reevaluate_before=None,
      categories=tuple(categories),
  )


def test_prompt_blocks_come_in_order():
  """Verifies role, context, metadata, then task A and task B."""
  text = prompt.build_prompt(
      _sheet_config(advertiser_context='A spirits brand.'), _VIDEO, 3
  )

  positions = [
      text.index('You are an objective'),
      text.index('Advertiser / Brand Context:\nA spirits brand.'),
      text.index('Video Metadata:\n- Video ID: vid_001'),
      text.index('=== TASK A'),
      text.index('=== TASK B'),
  ]
  assert positions == sorted(positions)
  assert '- Attached Verified Thumbnail Frames: 3' in text


def test_only_active_categories_are_listed_in_sheet_order():
  """Verifies Disabled categories are left out of the prompt."""
  text = prompt.build_prompt(_sheet_config(), _VIDEO, 0)

  assert text.endswith(
      '- `kids`: Content made for kids.\n- `violence_gore`: Graphic violence.'
  )
  assert 'profanity' not in text


def test_tasks_and_context_follow_the_sheet():
  """Verifies each block appears only when the Sheet switches it on."""
  age_only = prompt.build_prompt(_sheet_config(categories=()), _VIDEO, 1)
  assert '=== TASK A' in age_only
  assert '=== TASK B' not in age_only

  categories_only = prompt.build_prompt(
      _sheet_config(age_check_mode='Disabled'), _VIDEO, 1
  )
  assert '=== TASK A' not in categories_only
  assert '=== TASK B' in categories_only

  assert 'Advertiser / Brand Context' not in prompt.build_prompt(
      _sheet_config(), _VIDEO, 1
  )


def test_the_age_threshold_does_not_change_what_gemini_is_asked():
  """Verifies max_minor_age is applied in code, not sent in the request."""
  younger = _sheet_config(max_minor_age=18)
  older = _sheet_config(max_minor_age=29)

  assert prompt.build_prompt(younger, _VIDEO, 2) == prompt.build_prompt(
      older, _VIDEO, 2
  )
  assert prompt.build_response_schema(younger) == (
      prompt.build_response_schema(older)
  )


def test_line_breaks_typed_in_a_description_are_joined():
  """Verifies a multi-line Sheet cell stays on one line of the list."""
  categories = (
      config_lib.Category(
          'kids', 'Shadow', 0.7, 'Nursery rhymes,\n  toddlers\n\nand toys.'
      ),
  )
  text = prompt.build_prompt(_sheet_config(categories=categories), _VIDEO, 0)

  assert text.endswith('- `kids`: Nursery rhymes, toddlers and toys.')


def test_metadata_is_bounded_and_blanks_are_shown():
  """Verifies long descriptions are cut and empty fields read '(none)'."""
  video = {
      'video_id': 'vid_002',
      'title': '',
      'description': 'x' * (prompt.MAX_DESCRIPTION_CHARS + 50),
      'tags': ['a', '', 'b'],
  }
  text = prompt.build_prompt(_sheet_config(), video, 0)

  assert '- Title: (none)\n' in text
  assert f'- Description: {"x" * prompt.MAX_DESCRIPTION_CHARS}\n' in text
  assert '- Tags: a, b' in text


def test_response_schema_asks_only_for_active_checks():
  """Verifies the schema lists persons, the active categories and reasoning."""
  schema = prompt.build_response_schema(_sheet_config())

  assert list(schema['properties']) == ['persons', 'categories', 'reasoning']
  assert schema['required'] == ['persons', 'categories', 'reasoning']
  categories = schema['properties']['categories']
  assert list(categories['properties']) == ['kids', 'violence_gore']
  assert categories['required'] == ['kids', 'violence_gore']
  person = schema['properties']['persons']['items']
  assert person['required'] == ['description', 'is_real_human', 'estimated_age']
  assert person['properties']['estimated_age']['nullable']

  age_only = prompt.build_response_schema(_sheet_config(categories=()))
  assert list(age_only['properties']) == ['persons', 'reasoning']
  categories_only = prompt.build_response_schema(
      _sheet_config(age_check_mode='Disabled')
  )
  assert list(categories_only['properties']) == ['categories', 'reasoning']
