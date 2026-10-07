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

"""Builds what Gemini is asked for one video: the prompt and the answer schema.

What is fixed here and what comes from the Config Sheet:

  Fixed, in this file                     From the Config Sheet
  --------------------------------------  -------------------------------------
  The evaluator role                      Advertiser / brand context
  How to judge people and ages (task A)   Whether task A runs (age_check_mode)
  How to score categories (task B)        Which categories are scored and what
  The shape of the answer (JSON schema)     each one means (gemini_categories)
  Model settings (temperature, thinking,
    media resolution)

The age threshold is deliberately not part of the prompt. Gemini estimates
ages, and the threshold is applied in code when the results are collected, so
changing `max_minor_age` never changes what Gemini is asked.

Bump DETECTOR_VERSION whenever the wording or the schema in this file changes,
so that results produced under different instructions can be told apart in the
detection table.
"""

from typing import Any, Mapping

from gemini_video_evaluator.utils import config as config_lib

DETECTOR_VERSION = '2026.09-v2'

# Fixed rather than configurable: changing any of these changes the results, so
# it is a code change that comes with a DETECTOR_VERSION bump.
TEMPERATURE = 0.0
THINKING_BUDGET = 0
# Vision tokenization resolution ('MEDIA_RESOLUTION_HIGH' = 1,064 tokens/image,
# 'MEDIA_RESOLUTION_MEDIUM' = 540 tokens/image, 'MEDIA_RESOLUTION_LOW' = 266
# tokens/image on gemini-3.8-flash). 'MEDIA_RESOLUTION_MEDIUM' matches HIGH
# within run-to-run variance while cutting image tokens by 49.2% (-27.3% total
# evaluation cost); 'MEDIA_RESOLUTION_LOW' degrades adolescent (13-17) recall.
MEDIA_RESOLUTION = 'MEDIA_RESOLUTION_MEDIUM'

MAX_DESCRIPTION_CHARS = 2000

_ROLE = (
    'You are an objective multimodal brand-safety and regulatory compliance'
    ' evaluator for YouTube advertising placements.'
)

_AGE_TASK = """\
=== TASK A: PERSON & AGE EVALUATION (THUMBNAIL VISUALS) ===
Examine every provided video thumbnail frame carefully for any visible human
beings or character depictions:
1. List each distinct person/character in `persons`.
2. Set `is_real_human = true` ONLY for real photographed/recorded human
   beings (including real human faces composited onto thumbnails).
3. Set `is_real_human = false` for 2D/3D cartoons, anime characters,
   video-game avatars (e.g. Roblox, Minecraft, Fortnite, Sims), dolls,
   statues, toys, or illustrated drawings, and set their `estimated_age`
   to null.
4. For every real human (`is_real_human = true`), provide a concise visual
   description (`description`), which frame(s) they appear in, and an
   integer `estimated_age` in years (e.g. 1 for a toddler, 8 for a child,
   25 for an adult)."""

_CATEGORY_TASK = """\
=== TASK B: CONTENT CATEGORY SUITABILITY (THUMBNAILS + METADATA) ===
Evaluate the combined visual thumbnail frames and text metadata (title,
description, tags) against each configured content category.
For every category, output a monotonic float `score` from 0.0 to 1.0:
- 0.0 means the category is completely absent / safe for standard ads.
- 1.0 means the video definitely belongs to the category or violates the
  suitability policy.
CRITICAL: Higher score ALWAYS means stronger presence of the restricted
category or higher unsuitability. Never use 0.9+ to mean "90% confident
the video is safe".

Categories to score (0.0 = absent/safe, 1.0 = definite match/violation):"""


def _text_or_none(value: Any) -> str:
  """Renders an empty metadata value as '(none)' so the prompt shows it."""
  text = str(value or '').strip()
  return text or '(none)'


def _metadata_block(video: Mapping[str, Any], verified_image_count: int) -> str:
  """Lists the video's metadata, one field per line."""
  description = str(video.get('description') or '').strip()
  tags = video.get('tags')
  if isinstance(tags, (list, tuple)):
    tags = ', '.join(str(tag) for tag in tags if tag)
  return '\n'.join([
      'Video Metadata:',
      f'- Video ID: {_text_or_none(video.get("video_id"))}',
      f'- Attached Verified Thumbnail Frames: {verified_image_count}',
      f'- Title: {_text_or_none(video.get("title"))}',
      f'- Description: {_text_or_none(description[:MAX_DESCRIPTION_CHARS])}',
      f'- Tags: {_text_or_none(tags)}',
  ])


def build_prompt(
    sheet_config: config_lib.SheetConfig,
    video: Mapping[str, Any],
    verified_image_count: int,
) -> str:
  """Builds the text part of the request for one video."""
  blocks = [_ROLE]
  if sheet_config.advertiser_context:
    blocks.append(
        f'Advertiser / Brand Context:\n{sheet_config.advertiser_context}'
    )
  blocks.append(_metadata_block(video, verified_image_count))
  if sheet_config.age_check_active:
    blocks.append(_AGE_TASK)
  if sheet_config.active_categories:
    # Line breaks typed inside a Sheet cell would break the list apart, so
    # each description is put on one line.
    category_lines = [
        f'- `{cat.name}`: {" ".join(cat.description.split())}'
        for cat in sheet_config.active_categories
    ]
    blocks.append('\n'.join([_CATEGORY_TASK, *category_lines]))
  return '\n\n'.join(blocks)


def build_response_schema(
    sheet_config: config_lib.SheetConfig,
) -> dict[str, Any]:
  """Builds the JSON schema Gemini's answer must follow."""
  # Asks only for what is switched on in the Sheet. The schema is stored inside
  # every request, which is how the collection step later knows which checks a
  # row asked for without reading the Sheet.
  properties: dict[str, Any] = {}
  if sheet_config.age_check_active:
    properties['persons'] = {
        'type': 'ARRAY',
        'description': (
            'All visible people or character depictions across the attached'
            ' thumbnail frames.'
        ),
        'items': {
            'type': 'OBJECT',
            'properties': {
                'description': {
                    'type': 'STRING',
                    'description': (
                        'Concise visual description of the person or character'
                        ' and which frame they appear in.'
                    ),
                },
                'is_real_human': {
                    'type': 'BOOLEAN',
                    'description': (
                        'True ONLY for real photographed/recorded human beings.'
                        ' False for cartoons, anime, Roblox/Minecraft/game'
                        ' avatars, toys, statues, or illustrations.'
                    ),
                },
                'estimated_age': {
                    'type': 'INTEGER',
                    'nullable': True,
                    'description': (
                        'Estimated chronological age in years if is_real_human'
                        ' is true; null if is_real_human is false.'
                    ),
                },
            },
            'required': ['description', 'is_real_human', 'estimated_age'],
        },
    }
  if sheet_config.active_categories:
    names = [cat.name for cat in sheet_config.active_categories]
    properties['categories'] = {
        'type': 'OBJECT',
        'properties': {
            name: {
                'type': 'OBJECT',
                'properties': {
                    'score': {
                        'type': 'NUMBER',
                        'description': (
                            'Monotonic score from 0.0 (absent/safe) to 1.0'
                            f' (definite match) for category {name}.'
                        ),
                    },
                },
                'required': ['score'],
            }
            for name in names
        },
        'required': names,
    }
  properties['reasoning'] = {
      'type': 'STRING',
      'description': (
          'Concise factual explanation summarizing the visual frames and'
          ' metadata evidence.'
      ),
  }
  return {
      'type': 'OBJECT',
      'properties': properties,
      'required': list(properties),
  }
