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

"""Unit tests for gemini_video_evaluator.utils.batch."""

import json
import types

from gemini_video_evaluator.utils import batch as batch_lib
from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import prompt
from gemini_video_evaluator.utils import sql
import pytest

_MODEL = 'gemini-3.8-flash'

_VIDEO = {
    'video_id': 'vid_001',
    'title': 'Our day at the park',
    'description': 'A family vlog.',
    'tags': 'family, vlog',
}
_URLS = [
    'https://i.ytimg.com/vi/vid_001/hqdefault.jpg',
    'https://i.ytimg.com/vi/vid_001/hq1.jpg',
]

_ANSWER = {
    'persons': [
        {
            'description': 'Adult presenter',
            'is_real_human': True,
            'estimated_age': 30,
        },
        {
            'description': 'Child in frame 2',
            'is_real_human': True,
            'estimated_age': 9,
        },
        {
            'description': 'Cartoon bear',
            'is_real_human': False,
            'estimated_age': None,
        },
    ],
    'categories': {
        'kids': {'score': 0.91},
        'violence_gore': {'score': 0.02},
    },
    'reasoning': 'A child and a cartoon bear in a park.',
}


def _sheet_config(age_check_mode='Shadow', categories=None, max_minor_age=18):
  if categories is None:
    categories = (
        config_lib.Category('kids', 'Exclude', 0.8, 'Content made for kids.'),
        config_lib.Category('violence_gore', 'Shadow', 0.7, 'Violence.'),
    )
  return config_lib.SheetConfig(
      age_check_mode=age_check_mode,
      max_minor_age=max_minor_age,
      advertiser_context='',
      max_videos_per_batch=500,
      reevaluate_before=None,
      categories=tuple(categories),
  )


def _raw_response(text):
  """Wraps model text the way Batch Inference stores a response."""
  return json.dumps({'candidates': [{'content': {'parts': [{'text': text}]}}]})


def _response(answer):
  return _raw_response(json.dumps(answer))


def _output_row(answer, sheet_config=None, **overrides):
  """Builds an output row: the input row as sent, plus the model's answer."""
  row = batch_lib.build_batch_input_row(
      video=_VIDEO,
      verified_urls=_URLS,
      sheet_config=sheet_config or _sheet_config(),
      run_id='run-1',
  )
  row.update({'status': '', 'response': _response(answer)})
  row.update(overrides)
  return row


def _by_taxonomy(evaluation):
  return {detection.taxonomy: detection for detection in evaluation.detections}


def _with_persons(*persons):
  return {**_ANSWER, 'persons': list(persons)}


def _person(age, is_real_human=True):
  return {
      'description': 'A person',
      'is_real_human': is_real_human,
      'estimated_age': age,
  }


def test_input_row_carries_everything_needed_to_read_the_answer():
  """Verifies the stored request, threshold and version of an input row."""
  sheet_config = _sheet_config(max_minor_age=21)
  row = batch_lib.build_batch_input_row(_VIDEO, _URLS, sheet_config, 'run-1')

  assert row['video_id'] == 'vid_001'
  assert row['run_id'] == 'run-1'
  assert row['detector_version'] == prompt.DETECTOR_VERSION
  assert row['max_minor_age'] == 21
  assert row['verified_thumbnail_count'] == 2
  request = json.loads(row['request'])
  parts = request['contents'][0]['parts']
  assert [part['fileData']['fileUri'] for part in parts[:-1]] == _URLS
  assert parts[-1]['text'] == prompt.build_prompt(sheet_config, _VIDEO, 2)
  generation = request['generationConfig']
  assert generation['temperature'] == prompt.TEMPERATURE
  assert generation['mediaResolution'] == prompt.MEDIA_RESOLUTION
  assert generation['mediaResolution'] == 'MEDIA_RESOLUTION_MEDIUM'
  assert generation['thinkingConfig'] == {
      'thinkingBudget': prompt.THINKING_BUDGET
  }
  assert generation['responseMimeType'] == 'application/json'
  assert generation['responseSchema'] == prompt.build_response_schema(
      sheet_config
  )


def test_input_row_without_thumbnails_sends_text_only():
  """Verifies a video with no verified thumbnail sends no image URL."""
  row = batch_lib.build_batch_input_row(_VIDEO, [], _sheet_config(), 'run-1')

  assert row['verified_thumbnail_count'] == 0
  parts = json.loads(row['request'])['contents'][0]['parts']
  assert len(parts) == 1
  assert 'text' in parts[0]


def test_requested_evaluations_round_trips_the_sheet():
  """Verifies the checks asked for are read back from the stored request."""
  row = batch_lib.build_batch_input_row(_VIDEO, _URLS, _sheet_config(), 'r')

  requested = batch_lib.requested_evaluations(row['request'])

  assert requested == batch_lib.RequestedEvaluations(
      age_check=True, categories=('kids', 'violence_gore')
  )
  assert batch_lib.requested_evaluations(json.loads(row['request'])) == (
      requested
  )


def test_requested_evaluations_reads_requests_of_the_first_version():
  """Verifies batches sent before this version can still be collected."""
  legacy_request = {
      'contents': [{'role': 'user', 'parts': [{'text': 'prompt'}]}],
      'generationConfig': {
          'responseSchema': {
              'type': 'OBJECT',
              'properties': {
                  'persons': {'type': 'ARRAY'},
                  'categories': {
                      'type': 'OBJECT',
                      'properties': {
                          'kids': {'type': 'OBJECT'},
                          'brand_safety_unsuitable': {'type': 'OBJECT'},
                      },
                  },
                  'reasoning': {'type': 'STRING'},
              },
          }
      },
  }

  requested = batch_lib.requested_evaluations(json.dumps(legacy_request))

  assert requested == batch_lib.RequestedEvaluations(
      age_check=True, categories=('kids', 'brand_safety_unsuitable')
  )


@pytest.mark.parametrize(
    'request_value',
    [
        None,
        '',
        'not json',
        '[]',
        json.dumps({'generationConfig': {}}),
        json.dumps({
            'generationConfig': {
                'responseSchema': {'properties': {'reasoning': {}}}
            }
        }),
    ],
)
def test_requested_evaluations_rejects_unreadable_requests(request_value):
  """Verifies a request that does not show what was asked yields None."""
  assert batch_lib.requested_evaluations(request_value) is None


def test_a_complete_answer_becomes_detections():
  """Verifies the audit, age and category rows written for one video."""
  evaluation = batch_lib.parse_batch_output_row(_output_row(_ANSWER), _MODEL)

  assert evaluation.succeeded
  assert evaluation.video_id == 'vid_001'
  assert evaluation.detector_version == prompt.DETECTOR_VERSION
  by_taxonomy = _by_taxonomy(evaluation)
  assert list(by_taxonomy) == [
      'audit.evaluated',
      'age_eval.minor_presence',
      'content_eval.kids',
      'content_eval.violence_gore',
  ]
  audit = by_taxonomy['audit.evaluated']
  assert (audit.label, audit.score, audit.modality) == (
      'evaluated',
      0.0,
      'thumbnail_and_metadata',
  )
  minor = by_taxonomy['age_eval.minor_presence']
  assert (minor.label, minor.score, minor.modality) == (
      'minor_presence',
      1.0,
      'thumbnail',
  )
  evidence = json.loads(minor.evidence)
  assert evidence['min_real_human_age'] == 9
  assert evidence['max_minor_age_threshold'] == 18
  assert evidence['real_human_count'] == 2
  assert evidence['fictional_or_avatar_count'] == 1
  assert evidence['model'] == _MODEL
  kids = by_taxonomy['content_eval.kids']
  assert (kids.label, kids.score, kids.modality) == (
      'kids',
      0.91,
      'thumbnail_and_metadata',
  )
  for detection in evaluation.detections:
    assert detection.entity_type == 'video'
    assert detection.entity_id == 'vid_001'
    assert detection.source == 'first_party'
    assert detection.detector == 'gemini_thumbnail_metadata_eval'
    assert detection.detector_version == prompt.DETECTOR_VERSION


@pytest.mark.parametrize(
    ('youngest', 'max_minor_age', 'expected_score'),
    [(18, 18, 1.0), (19, 18, 0.0), (19, 21, 1.0), (22, 21, 0.0)],
)
def test_minor_presence_uses_the_threshold_sent_with_the_row(
    youngest, max_minor_age, expected_score
):
  """Verifies the threshold is inclusive and read from the row itself."""
  row = _output_row(
      _with_persons(_person(40), _person(youngest)),
      _sheet_config(max_minor_age=max_minor_age),
  )

  evaluation = batch_lib.parse_batch_output_row(row, _MODEL)

  minor = _by_taxonomy(evaluation)['age_eval.minor_presence']
  assert minor.score == expected_score


def test_rows_sent_before_the_threshold_was_stored_use_18():
  """Verifies a row without max_minor_age falls back to the default threshold."""
  row = _output_row(_with_persons(_person(19)), _sheet_config(max_minor_age=21))
  assert (
      _by_taxonomy(batch_lib.parse_batch_output_row(row, _MODEL))[
          'age_eval.minor_presence'
      ].score
      == 1.0
  )

  del row['max_minor_age']
  minor = _by_taxonomy(batch_lib.parse_batch_output_row(row, _MODEL))[
      'age_eval.minor_presence'
  ]

  assert minor.score == 0.0
  assert json.loads(minor.evidence)['max_minor_age_threshold'] == 18


def test_cartoons_and_avatars_are_not_counted_as_minors():
  """Verifies an age given for a non-human is ignored."""
  row = _output_row(_with_persons(_person(8, is_real_human=False)))

  minor = _by_taxonomy(batch_lib.parse_batch_output_row(row, _MODEL))[
      'age_eval.minor_presence'
  ]

  assert minor.score == 0.0
  evidence = json.loads(minor.evidence)
  assert evidence['min_real_human_age'] is None
  assert evidence['real_human_count'] == 0
  assert evidence['fictional_or_avatar_count'] == 1
  assert evidence['persons'][0]['estimated_age'] is None


def test_real_people_without_a_usable_age_are_counted_but_not_scored():
  """Verifies a missing or impossible age does not make a video a match."""
  row = _output_row(_with_persons(_person(None), _person(400)))

  minor = _by_taxonomy(batch_lib.parse_batch_output_row(row, _MODEL))[
      'age_eval.minor_presence'
  ]

  assert minor.score == 0.0
  evidence = json.loads(minor.evidence)
  assert evidence['real_human_count'] == 2
  assert evidence['min_real_human_age'] is None


def test_only_the_checks_that_were_asked_for_are_recorded():
  """Verifies extra persons or categories in an answer are ignored."""
  sheet_config = _sheet_config(
      age_check_mode='Disabled',
      categories=(config_lib.Category('kids', 'Shadow', 0.7, 'Kids.'),),
  )

  evaluation = batch_lib.parse_batch_output_row(
      _output_row(_ANSWER, sheet_config), _MODEL
  )

  assert [d.taxonomy for d in evaluation.detections] == [
      'audit.evaluated',
      'content_eval.kids',
  ]


@pytest.mark.parametrize(
    ('raw_score', 'expected'),
    [(1.3, 1.0), (-0.2, 0.0), (0.123456, 0.1235), (1, 1.0)],
)
def test_category_scores_are_clamped_and_rounded(raw_score, expected):
  """Verifies scores are kept within 0.0 to 1.0, to 4 decimal places."""
  answer = {
      **_ANSWER,
      'categories': {
          'kids': {'score': raw_score},
          'violence_gore': {'score': 0.0},
      },
  }

  evaluation = batch_lib.parse_batch_output_row(_output_row(answer), _MODEL)

  assert _by_taxonomy(evaluation)['content_eval.kids'].score == expected


def test_thought_parts_of_the_response_are_skipped():
  """Verifies the answer is taken from the first part that is not a thought."""
  response = json.dumps({
      'candidates': [{
          'content': {
              'parts': [
                  {'text': 'Let me think.', 'thought': True},
                  {'text': json.dumps(_ANSWER)},
              ]
          }
      }]
  })

  evaluation = batch_lib.parse_batch_output_row(
      _output_row(_ANSWER, response=response), _MODEL
  )

  assert evaluation.succeeded


@pytest.mark.parametrize(
    ('overrides', 'expected_error'),
    [
        ({'video_id': ''}, 'no video_id'),
        (
            {'status': '{"code": 3, "message": "URL_ERROR-ERROR_NOT_FOUND"}'},
            'URL_ERROR-ERROR_NOT_FOUND',
        ),
        ({'request': '{}'}, 'does not show which evaluations were requested'),
        ({'response': None}, 'Missing or empty candidate text'),
        (
            {'response': json.dumps({'candidates': []})},
            'Missing or empty candidate text',
        ),
        ({'response': _raw_response('not json')}, 'Invalid JSON'),
        ({'response': _raw_response('[1, 2]')}, 'root must be an object'),
        (
            {'response': _response({**_ANSWER, 'persons': None})},
            "no 'persons' list",
        ),
        (
            {
                'response': _response(
                    {**_ANSWER, 'categories': {'kids': {'score': 0.5}}}
                )
            },
            "no numeric score for category 'violence_gore'",
        ),
        (
            {
                'response': _response({
                    **_ANSWER,
                    'categories': {
                        'kids': {'score': True},
                        'violence_gore': {'score': 0.1},
                    },
                })
            },
            "no numeric score for category 'kids'",
        ),
        (
            {
                'response': _response({
                    **_ANSWER,
                    'categories': {
                        'kids': {'score': 'high'},
                        'violence_gore': {'score': 0.1},
                    },
                })
            },
            "no numeric score for category 'kids'",
        ),
    ],
)
def test_unusable_answers_fail_rather_than_being_guessed(
    overrides, expected_error
):
  """Verifies an incomplete answer yields no detections at all."""
  evaluation = batch_lib.parse_batch_output_row(
      _output_row(_ANSWER, **overrides), _MODEL
  )

  assert not evaluation.succeeded
  assert expected_error in evaluation.error_message
  assert not evaluation.detections


def test_a_failed_answer_is_recorded_under_the_audit_row_key():
  """Verifies a later success overwrites the failure in the MERGE."""
  failed = batch_lib.parse_batch_output_row(
      _output_row(_ANSWER, response=None), _MODEL
  )
  succeeded = batch_lib.parse_batch_output_row(_output_row(_ANSWER), _MODEL)

  record = batch_lib.build_failure_audit_record(failed, _MODEL)

  def merge_key(detection):
    return (
        detection.entity_type,
        detection.source,
        detection.detector,
        detection.entity_id,
        detection.modality,
        detection.label,
    )

  assert merge_key(record) == merge_key(
      _by_taxonomy(succeeded)[sql.TAXONOMY_AUDIT_EVALUATED]
  )
  assert record.taxonomy == sql.TAXONOMY_AUDIT_EVALUATED
  assert record.score == 0.0
  evidence = json.loads(record.evidence)
  assert evidence['status'] == 'failed'
  assert evidence['failed_attempts'] == 1
  assert evidence['model'] == _MODEL
  assert 'Missing or empty candidate text' in evidence['error']


def test_is_our_batch_job_checks_destination_and_display_name():
  """Verifies only this deployment's jobs are collected."""

  def job(uri, display_name='vet-gemini-video-eval-20260928_101000_abcd'):
    return types.SimpleNamespace(
        display_name=display_name, dest=types.SimpleNamespace(bigquery_uri=uri)
    )

  ours = 'bq://test-gcp-project.test_dataset._gemini_eval_output_20260928'
  assert batch_lib.is_our_batch_job(
      job(ours), 'test-gcp-project', 'test_dataset'
  )
  assert not batch_lib.is_our_batch_job(
      job(ours), 'other-project', 'test_dataset'
  )
  assert not batch_lib.is_our_batch_job(
      job(ours), 'test-gcp-project', 'other_dataset'
  )
  assert not batch_lib.is_our_batch_job(
      job('bq://test-gcp-project.test_dataset.some_table'),
      'test-gcp-project',
      'test_dataset',
  )
  assert not batch_lib.is_our_batch_job(
      job(ours, display_name='someone-elses-job'),
      'test-gcp-project',
      'test_dataset',
  )
  assert not batch_lib.is_our_batch_job(
      types.SimpleNamespace(display_name='', dest=None),
      'test-gcp-project',
      'test_dataset',
  )


def test_companion_tables_share_the_output_table_tag():
  """Verifies the input and staging tables are found from the output table."""
  assert batch_lib.derive_companion_tables(
      '_gemini_eval_output_20260928_101000_abcd'
  ) == (
      '_gemini_eval_input_20260928_101000_abcd',
      '_gemini_eval_staging_20260928_101000_abcd',
  )


def test_job_state_and_model_are_normalized():
  """Verifies enum, string and missing states, and model resource names."""
  assert batch_lib.normalize_job_state(None) == 'JOB_STATE_UNSPECIFIED'
  assert (
      batch_lib.normalize_job_state('JobState.JOB_STATE_RUNNING')
      == 'JOB_STATE_RUNNING'
  )
  assert (
      batch_lib.normalize_job_state(
          types.SimpleNamespace(name='JOB_STATE_SUCCEEDED')
      )
      == 'JOB_STATE_SUCCEEDED'
  )
  assert (
      batch_lib.model_id(
          types.SimpleNamespace(
              model='publishers/google/models/gemini-3.8-flash'
          )
      )
      == 'gemini-3.8-flash'
  )
  assert batch_lib.model_id(types.SimpleNamespace(model=None)) == 'unknown'


def test_completion_stats_are_formatted():
  """Verifies progress text before, during and after the first flush."""

  def job(**stats):
    return types.SimpleNamespace(
        completion_stats=types.SimpleNamespace(**stats)
    )

  assert (
      batch_lib.format_completion_stats(types.SimpleNamespace())
      == '0/? (0 ok, 0 failed)'
  )
  assert (
      batch_lib.format_completion_stats(
          job(successful_count=None, failed_count=None, incomplete_count=-1)
      )
      == '0/? (0 ok, 0 failed)'
  )
  assert (
      batch_lib.format_completion_stats(
          job(successful_count=8, failed_count=2, incomplete_count=5)
      )
      == '10/15 (8 ok, 2 failed)'
  )
