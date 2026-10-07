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

"""Unit tests for gemini_video_evaluator.main orchestration and HTTP handler."""

import dataclasses
import datetime
import json
import types
from unittest import mock

import flask
from gemini_video_evaluator import main
from gemini_video_evaluator.utils import batch as batch_lib
from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import thumbnails
from google.api_core import exceptions as api_exceptions
import pytest
from vet_common.logging import PipelineTelemetryContext

_PROJECT = 'test-gcp-project'
_DATASET = 'test_dataset'
_TAG = '20260928_101000_abcd1234'
_JOB_NAME = 'projects/1/locations/global/batchPredictionJobs/99'
_INPUT_TABLE = f'{batch_lib.INPUT_TABLE_PREFIX}{_TAG}'
_OUTPUT_TABLE = f'{batch_lib.OUTPUT_TABLE_PREFIX}{_TAG}'
_STAGING_TABLE = f'{batch_lib.STAGING_TABLE_PREFIX}{_TAG}'
_DETECTION_MERGE = f'MERGE `{_PROJECT}.{_DATASET}.detection` AS target'
_POLICY_MERGE = f'MERGE `{_PROJECT}.{_DATASET}.detection_policy` AS T'
_CANDIDATE_QUERY = 'WITH corpus AS'

_SETTINGS = [
    {'setting': 'age_check_mode', 'value': 'Shadow'},
    {'setting': 'max_minor_age', 'value': '18'},
]
_CATEGORIES = [
    {
        'category': 'kids',
        'mode': 'Exclude',
        'min_score': '0.8',
        'description': 'Content made for kids.',
    },
    {
        'category': 'violence_gore',
        'mode': 'Shadow',
        'min_score': '',
        'description': 'Graphic violence.',
    },
]

# The Sheet above as parsed, used to build the rows of a batch sent under it.
_SHEET_CONFIG = config_lib.SheetConfig(
    age_check_mode='Shadow',
    max_minor_age=18,
    advertiser_context='',
    max_videos_per_batch=500,
    reevaluate_before=None,
    categories=(
        config_lib.Category('kids', 'Exclude', 0.8, 'Content made for kids.'),
        config_lib.Category(
            'violence_gore', 'Shadow', 0.7, 'Graphic violence.'
        ),
    ),
)

_ANSWER = {
    'persons': [
        {'description': 'Child', 'is_real_human': True, 'estimated_age': 8}
    ],
    'categories': {'kids': {'score': 0.9}, 'violence_gore': {'score': 0.1}},
    'reasoning': 'A child playing.',
}


class _FakeQueryJob:
  """A finished query or load job."""

  def __init__(self, rows=(), error=None):
    self._rows = list(rows)
    self._error = error

  def result(self):
    if self._error:
      raise self._error
    return self._rows


class _FakeBigQuery:
  """Answers the evaluator's queries according to the table each one reads."""

  def __init__(
      self,
      settings=(),
      categories=(),
      output_rows=(),
      candidates=(),
      tables=(),
      settings_error=None,
  ):
    self.settings = list(settings)
    self.categories = list(categories)
    self.output_rows = list(output_rows)
    self.candidates = list(candidates)
    self.tables = set(tables)
    self.settings_error = settings_error
    self.queries = []
    self.loaded = {}
    self.deleted = []

  def query(self, query, job_config=None):
    self.queries.append((query, job_config))
    if '.gemini_settings`' in query:
      return _FakeQueryJob(self.settings, self.settings_error)
    if '.gemini_categories`' in query:
      return _FakeQueryJob(self.categories)
    if query.startswith('SELECT * FROM'):
      return _FakeQueryJob(self.output_rows)
    if query.startswith(_CANDIDATE_QUERY):
      return _FakeQueryJob(self.candidates)
    return _FakeQueryJob()

  def get_table(self, table_ref):
    if table_ref.rsplit('.', 1)[-1] not in self.tables:
      raise api_exceptions.NotFound(table_ref)
    return types.SimpleNamespace(expires=None)

  def update_table(self, table, fields):
    del table, fields

  def delete_table(self, table_ref, not_found_ok=False):
    del not_found_ok
    table = table_ref.rsplit('.', 1)[-1]
    self.deleted.append(table)
    self.tables.discard(table)

  def load_table_from_file(self, file_obj, table_ref, job_config=None):
    del job_config
    table = table_ref.rsplit('.', 1)[-1]
    self.loaded[table] = [
        json.loads(line) for line in file_obj.read().decode().splitlines()
    ]
    self.tables.add(table)
    return _FakeQueryJob()

  def executed(self, prefix):
    """Returns the (query, job_config) pairs whose query starts with prefix."""
    return [(q, c) for q, c in self.queries if q.startswith(prefix)]

  def read_sheet(self):
    return any('.gemini_settings`' in q for q, _ in self.queries)


class _FakeBatches:
  """The `batches` API of a GenAI client.

  `states` gives a job the states that successive `get` calls return; the
  last one repeats.
  """

  def __init__(self, jobs=(), states=None):
    self._jobs = {job.name: job for job in jobs}
    self._states = {name: list(seq) for name, seq in (states or {}).items()}
    self.get_calls = []
    self.created = []

  def list(self, config=None):
    del config
    return list(self._jobs.values())

  def get(self, name):
    self.get_calls.append(name)
    job = self._jobs[name]
    sequence = self._states.get(name)
    if sequence:
      job.state = sequence.pop(0) if len(sequence) > 1 else sequence[0]
    return job

  def create(self, model, src, config):
    self.created.append({'model': model, 'src': src, 'config': config})
    return types.SimpleNamespace(
        name='projects/1/locations/global/batchPredictionJobs/new',
        state='JOB_STATE_PENDING',
    )


def _genai(jobs=(), states=None):
  return types.SimpleNamespace(batches=_FakeBatches(jobs, states))


def _job(
    name=_JOB_NAME,
    state='JOB_STATE_SUCCEEDED',
    tag=_TAG,
    dataset=_DATASET,
    display_name=None,
):
  return types.SimpleNamespace(
      name=name,
      state=state,
      model='publishers/google/models/gemini-3.8-flash',
      display_name=display_name or f'{batch_lib.DISPLAY_NAME_PREFIX}{tag}',
      dest=types.SimpleNamespace(
          bigquery_uri=(
              f'bq://{_PROJECT}.{dataset}.{batch_lib.OUTPUT_TABLE_PREFIX}{tag}'
          )
      ),
      error=None,
      completion_stats=None,
  )


def _output_row(video_id, answer=None, **overrides):
  """Builds a batch output row: the input row as sent, plus the answer."""
  row = batch_lib.build_batch_input_row(
      video={'video_id': video_id, 'title': 'T', 'description': '', 'tags': ''},
      verified_urls=[f'https://i.ytimg.com/vi/{video_id}/hqdefault.jpg'],
      sheet_config=_SHEET_CONFIG,
      run_id='run-earlier',
  )
  row['status'] = ''
  row['response'] = json.dumps({
      'candidates': [
          {'content': {'parts': [{'text': json.dumps(answer or _ANSWER)}]}}
      ]
  })
  row.update(overrides)
  return row


def _telemetry():
  """A real telemetry context, with its steps recorded.

  Being real, it fails the test if step metadata cannot be serialized.

  Returns:
    A PipelineTelemetryContext instance with a wrapped log_step method.
  """
  telemetry = PipelineTelemetryContext(
      logger=main.logger, service_name='test-service', run_id='run-test'
  )
  telemetry.log_step = mock.MagicMock(wraps=telemetry.log_step)
  return telemetry


def _step(telemetry, name):
  (call,) = [
      c for c in telemetry.log_step.call_args_list if c.kwargs['step'] == name
  ]
  return call.kwargs


def _params(job_config):
  return {p.name: p.value for p in job_config.query_parameters}


def _run(bq, genai, **kwargs):
  return main.run_pipeline(
      project=_PROJECT,
      dataset=_DATASET,
      telemetry=kwargs.pop('telemetry', None) or _telemetry(),
      bq_client=bq,
      genai_client=genai,
      **kwargs,
  )


def test_schemas_match_the_rows_they_load():
  """Verifies the load schemas list exactly the fields of the loaded rows."""
  input_row = batch_lib.build_batch_input_row(
      {'video_id': 'v', 'title': '', 'description': '', 'tags': ''},
      [],
      _SHEET_CONFIG,
      'run',
  )

  assert {f.name for f in main.BATCH_INPUT_SCHEMA} == set(input_row)
  assert {f.name for f in main.DETECTION_STAGING_SCHEMA} == {
      f.name for f in dataclasses.fields(batch_lib.DetectionRecord)
  }


def test_reconcile_merges_answers_and_drops_the_batch_tables():
  """Verifies a finished batch is collected without reading the Sheet."""
  bq = _FakeBigQuery(
      output_rows=[_output_row('vid_001')],
      tables={_INPUT_TABLE, _OUTPUT_TABLE},
  )

  summary = main.reconcile_batches(bq, _genai([_job()]), _PROJECT, _DATASET)

  assert summary['reconciled_jobs'] == [_JOB_NAME]
  assert summary['videos_reconciled'] == 1
  assert summary['videos_failed'] == 0
  assert summary['failures_recorded'] == 0
  assert summary['detections_merged'] == 4
  assert [row['taxonomy'] for row in bq.loaded[_STAGING_TABLE]] == [
      'audit.evaluated',
      'age_eval.minor_presence',
      'content_eval.kids',
      'content_eval.violence_gore',
  ]
  # SELECT *, because batches sent by earlier versions lack some columns.
  assert bq.executed('SELECT * FROM')[0][0] == (
      f'SELECT * FROM `{_PROJECT}.{_DATASET}.{_OUTPUT_TABLE}`'
  )
  ((_, merge_config),) = bq.executed(_DETECTION_MERGE)
  assert 'run_ts' in _params(merge_config)
  assert bq.deleted == [_INPUT_TABLE, _OUTPUT_TABLE, _STAGING_TABLE]
  assert not bq.read_sheet()


def test_reconcile_records_unusable_answers_when_others_succeeded():
  """Verifies a video with a broken answer is not sent again every run."""
  bq = _FakeBigQuery(
      output_rows=[
          _output_row('vid_001'),
          _output_row('vid_002', response=None),
          _output_row('', response=None),
      ],
      tables={_INPUT_TABLE, _OUTPUT_TABLE},
  )

  summary = main.reconcile_batches(bq, _genai([_job()]), _PROJECT, _DATASET)

  assert summary['videos_reconciled'] == 1
  assert summary['videos_failed'] == 2
  assert summary['failures_recorded'] == 1
  assert summary['detections_merged'] == 5
  (failure,) = [
      row for row in bq.loaded[_STAGING_TABLE] if row['entity_id'] == 'vid_002'
  ]
  assert failure['taxonomy'] == 'audit.evaluated'
  assert json.loads(failure['evidence'])['status'] == 'failed'


def test_reconcile_records_nothing_when_no_answer_is_usable():
  """Verifies a batch that failed as a whole leaves its videos to be resent."""
  bq = _FakeBigQuery(
      output_rows=[
          _output_row('vid_001', response=None),
          _output_row('vid_002', status='{"code": 13}'),
      ],
      tables={_INPUT_TABLE, _OUTPUT_TABLE},
  )

  summary = main.reconcile_batches(bq, _genai([_job()]), _PROJECT, _DATASET)

  assert summary['reconciled_jobs'] == [_JOB_NAME]
  assert summary['videos_failed'] == 2
  assert summary['failures_recorded'] == 0
  assert summary['detections_merged'] == 0
  assert not bq.loaded
  assert not bq.executed(_DETECTION_MERGE)
  assert bq.deleted == [_INPUT_TABLE, _OUTPUT_TABLE, _STAGING_TABLE]


def test_reconcile_skips_batches_already_collected():
  """Verifies a succeeded job whose output table is gone is left alone."""
  bq = _FakeBigQuery()

  summary = main.reconcile_batches(bq, _genai([_job()]), _PROJECT, _DATASET)

  assert not summary['reconciled_jobs']
  assert not bq.queries
  assert not bq.deleted


def test_reconcile_cleans_up_failed_batches_once():
  """Verifies a failed job's tables are dropped and its videos not marked."""
  bq = _FakeBigQuery(tables={_INPUT_TABLE})
  genai = _genai([_job(state='JOB_STATE_FAILED')])

  summary = main.reconcile_batches(bq, genai, _PROJECT, _DATASET)

  assert summary['cleaned_failed_jobs'] == [_JOB_NAME]
  assert summary['detections_merged'] == 0
  assert bq.deleted == [_INPUT_TABLE, _OUTPUT_TABLE, _STAGING_TABLE]

  again = main.reconcile_batches(bq, genai, _PROJECT, _DATASET)

  assert not again['cleaned_failed_jobs']


def test_reconcile_lists_running_batches_and_ignores_other_jobs():
  """Verifies only this deployment's jobs are looked up and reported."""
  ours = _job(state='JOB_STATE_RUNNING')
  other_dataset = _job(name='jobs/2', dataset='other_dataset')
  other_tool = _job(name='jobs/3', display_name='someone-elses-job')
  genai = _genai([ours, other_dataset, other_tool])

  summary = main.reconcile_batches(_FakeBigQuery(), genai, _PROJECT, _DATASET)

  assert summary['active_job_names'] == [_JOB_NAME]
  assert genai.batches.get_calls == [_JOB_NAME]


def test_reconcile_inspects_a_bounded_number_of_jobs():
  """Verifies the job listing, which pages automatically, is cut off."""
  jobs = [
      _job(name=f'jobs/{i}', state='JOB_STATE_RUNNING', tag=f'tag_{i}')
      for i in range(3)
  ]

  summary = main.reconcile_batches(
      _FakeBigQuery(), _genai(jobs), _PROJECT, _DATASET, max_scan_jobs=2
  )

  assert summary['active_job_names'] == ['jobs/0', 'jobs/1']


def test_an_unreadable_tab_is_reported_as_a_sheet_problem():
  """Verifies a missing tab or missing access reads as a fixable problem."""
  bq = _FakeBigQuery(
      settings_error=api_exceptions.BadRequest('Unable to parse range'),
      categories=_CATEGORIES,
  )

  with pytest.raises(config_lib.SheetConfigError) as exc_info:
    main.load_sheet_config(bq, _PROJECT, _DATASET)

  (problem,) = exc_info.value.problems
  assert problem.startswith(
      'The gemini_settings tab could not be read (Unable to parse range).'
  )
  assert "a tab named exactly 'gemini_settings'" in problem
  assert 'service account can view the Sheet' in problem


def test_a_sheet_problem_stops_the_run_after_finished_batches_are_saved():
  """Verifies collection runs first, and nothing else happens after it."""
  bq = _FakeBigQuery(
      settings=_SETTINGS,
      categories=[{**_CATEGORIES[0], 'min_score': '0.07'}],
      output_rows=[_output_row('vid_001')],
      tables={_INPUT_TABLE, _OUTPUT_TABLE},
  )
  genai = _genai([_job()])
  telemetry = _telemetry()

  with pytest.raises(config_lib.SheetConfigError):
    _run(bq, genai, telemetry=telemetry)

  assert bq.executed(_DETECTION_MERGE)
  assert not bq.executed(_POLICY_MERGE)
  assert not bq.executed(_CANDIDATE_QUERY)
  assert not genai.batches.created
  read_config = _step(telemetry, 'READ_CONFIG')
  assert read_config['status'] == 'FAILED'
  (problem,) = read_config['metadata']['problems']
  assert "the min_score of kids is '0.07'" in problem


def test_the_exclusion_rules_are_synced_from_the_sheet():
  """Verifies one policy per check is sent to the sync MERGE."""
  bq = _FakeBigQuery(settings=_SETTINGS, categories=_CATEGORIES)

  result = _run(bq, _genai())

  assert result['status'] == 'NO_CANDIDATES'
  ((_, sync_config),) = bq.executed(_POLICY_MERGE)
  (policies,) = sync_config.query_parameters
  assert policies.name == 'policies'
  assert [p.struct_values for p in policies.values] == [
      {
          'policy_id': 'first_party_gemini_minor_presence',
          'taxonomy_prefix': 'age_eval.minor_presence',
          'min_score': 1.0,
          'action': 'shadow',
          'enabled': True,
          'notes': mock.ANY,
      },
      {
          'policy_id': 'first_party_gemini_content_kids',
          'taxonomy_prefix': 'content_eval.kids',
          'min_score': 0.8,
          'action': 'exclude',
          'enabled': True,
          'notes': mock.ANY,
      },
      {
          'policy_id': 'first_party_gemini_content_violence_gore',
          'taxonomy_prefix': 'content_eval.violence_gore',
          'min_score': 0.7,
          'action': 'shadow',
          'enabled': True,
          'notes': mock.ANY,
      },
  ]
  ((_, age_update_config),) = bq.executed(
      f'UPDATE `{_PROJECT}.{_DATASET}.detection`'
  )
  (max_minor_age_param,) = age_update_config.query_parameters
  assert max_minor_age_param.name == 'max_minor_age'
  assert max_minor_age_param.type_ == 'INT64'
  assert max_minor_age_param.value == 18


def test_nothing_is_sent_when_every_check_is_disabled():
  """Verifies the rules are still synced, so disabling takes effect."""
  bq = _FakeBigQuery(
      settings=[{'setting': 'age_check_mode', 'value': 'Disabled'}],
      categories=[{**_CATEGORIES[0], 'mode': 'Disabled'}],
  )

  result = _run(bq, _genai())

  assert result['status'] == 'DISPATCH_DISABLED'
  assert 'every category are Disabled' in result['reason']
  ((_, sync_config),) = bq.executed(_POLICY_MERGE)
  assert [
      p.struct_values['enabled'] for p in sync_config.query_parameters[0].values
  ] == [False, False]
  assert not bq.executed(_CANDIDATE_QUERY)


def test_nothing_is_sent_when_max_videos_is_zero():
  """Verifies max_videos_per_batch = 0 pauses sending."""
  bq = _FakeBigQuery(
      settings=[*_SETTINGS, {'setting': 'max_videos_per_batch', 'value': '0'}],
      categories=_CATEGORIES,
  )

  result = _run(bq, _genai())

  assert result['status'] == 'DISPATCH_DISABLED'
  assert 'max_videos_per_batch is 0' in result['reason']
  assert not bq.executed(_CANDIDATE_QUERY)


def test_no_new_batch_is_sent_while_one_is_running():
  """Verifies a running batch holds back the next one."""
  bq = _FakeBigQuery(settings=_SETTINGS, categories=_CATEGORIES)
  genai = _genai([_job(state='JOB_STATE_RUNNING')])

  result = _run(bq, genai)

  assert result['status'] == 'IN_FLIGHT_THROTTLED'
  assert result['reconcile']['active_job_names'] == [_JOB_NAME]
  assert bq.executed(_POLICY_MERGE)
  assert not bq.executed(_CANDIDATE_QUERY)
  assert not genai.batches.created


def test_the_batch_size_is_capped_and_warned_about():
  """Verifies the deployment limit applies and the warning is returned."""
  bq = _FakeBigQuery(
      settings=[
          *_SETTINGS,
          {'setting': 'max_videos_per_batch', 'value': '100000'},
      ],
      categories=_CATEGORIES,
  )

  result = _run(bq, _genai())

  assert result['status'] == 'NO_CANDIDATES'
  assert len(result['warnings']) == 1
  ((_, candidate_config),) = bq.executed(_CANDIDATE_QUERY)
  assert _params(candidate_config) == {
      'max_videos': main.MAX_VIDEOS_PER_BATCH,
      'reevaluate_before': None,
  }


def test_the_candidate_query_gets_the_cap_and_reevaluate_before():
  """Verifies the test cap and the Sheet's reevaluate_before reach SQL."""
  bq = _FakeBigQuery(
      settings=[
          *_SETTINGS,
          {'setting': 'max_videos_per_batch', 'value': '100'},
          {'setting': 'reevaluate_before', 'value': '2026-09-01'},
      ],
      categories=_CATEGORIES,
  )

  _run(bq, _genai(), max_videos_cap=10)

  ((_, candidate_config),) = bq.executed(_CANDIDATE_QUERY)
  assert _params(candidate_config) == {
      'max_videos': 10,
      'reevaluate_before': datetime.datetime(
          2026, 9, 1, tzinfo=datetime.timezone.utc
      ),
  }


@mock.patch.object(thumbnails, 'probe_video_thumbnails', autospec=True)
def test_a_batch_is_sent_with_verified_thumbnails_and_the_threshold(
    mock_probe,
):
  """Verifies the staged requests and the batch job that is started."""
  bq = _FakeBigQuery(
      settings=[*_SETTINGS[:1], {'setting': 'max_minor_age', 'value': '21'}],
      categories=_CATEGORIES,
      candidates=[
          {
              'video_id': 'vid_001',
              'title': 'New video',
              'description': '',
              'tags': '',
              'reevaluation': False,
          },
          {
              'video_id': 'vid_002',
              'title': 'Old video',
              'description': '',
              'tags': '',
              'reevaluation': True,
          },
      ],
  )
  mock_probe.return_value = {
      'vid_001': thumbnails.ThumbnailProbeResult(
          video_id='vid_001',
          verified_urls=(
              'https://i.ytimg.com/vi/vid_001/hqdefault.jpg',
              'https://i.ytimg.com/vi/vid_001/hq1.jpg',
          ),
          slots_probed=4,
          slots_valid=2,
      )
  }
  genai = _genai()
  telemetry = _telemetry()

  result = _run(bq, genai, telemetry=telemetry)

  assert result['status'] == 'DISPATCHED'
  assert result['candidates_discovered'] == 2
  assert result['dispatched_job']['videos_submitted'] == 2
  assert result['dispatched_job']['total_verified_thumbnails'] == 2
  assert len(bq.loaded) == 1
  input_table = next(iter(bq.loaded))
  assert input_table.startswith(batch_lib.INPUT_TABLE_PREFIX)
  rows = bq.loaded[input_table]
  assert [row['video_id'] for row in rows] == ['vid_001', 'vid_002']
  assert [row['verified_thumbnail_count'] for row in rows] == [2, 0]
  assert {row['max_minor_age'] for row in rows} == {21}
  tag = input_table[len(batch_lib.INPUT_TABLE_PREFIX) :]
  (created,) = genai.batches.created
  assert created['model'] == main.GEMINI_MODEL
  assert created['src'] == f'bq://{_PROJECT}.{_DATASET}.{input_table}'
  output_table = f'{batch_lib.OUTPUT_TABLE_PREFIX}{tag}'
  assert created['config'] == {
      'display_name': f'{batch_lib.DISPLAY_NAME_PREFIX}{tag}',
      'dest': f'bq://{_PROJECT}.{_DATASET}.{output_table}',
  }
  discover = _step(telemetry, 'DISCOVER_CANDIDATES')
  assert discover['metadata']['new_videos'] == 1
  assert discover['metadata']['reevaluations'] == 1


@mock.patch.object(main.time, 'sleep', autospec=True)
def test_a_blocking_run_waits_for_the_running_batch_and_collects_it(
    mock_sleep,
):
  """Verifies --blocking waits for an in-flight batch instead of skipping."""
  bq = _FakeBigQuery(
      settings=_SETTINGS,
      categories=_CATEGORIES,
      output_rows=[_output_row('vid_001')],
      tables={_INPUT_TABLE, _OUTPUT_TABLE},
  )
  genai = _genai(
      [_job(state='JOB_STATE_RUNNING')],
      states={_JOB_NAME: ['JOB_STATE_RUNNING', 'JOB_STATE_SUCCEEDED']},
  )

  result = _run(bq, genai, blocking=True, poll_interval_seconds=0.5)

  assert result['status'] == 'RECONCILED_IN_FLIGHT'
  assert result['reconcile']['active_job_names'] == [_JOB_NAME]
  assert result['blocking_reconcile']['reconciled_jobs'] == [_JOB_NAME]
  assert result['blocking_reconcile']['detections_merged'] == 4
  mock_sleep.assert_called_once_with(0.5)


def _post(body, content_type='application/json'):
  app = flask.Flask(__name__)
  with app.test_request_context(
      '/', method='POST', data=body, content_type=content_type
  ):
    response, code = main.main(flask.request)
    return response.get_json(), code


@pytest.mark.parametrize(
    ('body', 'expected'),
    [
        (b'{not-json', 'The request body is not valid JSON.'),
        (b'[]', "[] is not of type 'object'."),
        (b'{"blocking": false}', "('blocking' was unexpected)."),
    ],
)
def test_http_rejects_anything_but_an_empty_request(body, expected):
  """Verifies leftover parameters are refused, with how to fix the request."""
  with mock.patch.object(main, 'run_pipeline', autospec=True) as mock_run:
    payload, code = _post(body)

  assert code == 400
  assert payload['status'] == 'error'
  assert expected in payload['reason']
  assert 'Send an empty JSON object, {}.' in payload['reason']
  mock_run.assert_not_called()


@pytest.mark.parametrize('body', [b'', b'{}'])
def test_http_returns_the_run_summary(body):
  """Verifies an empty request runs the pipeline and returns its summary."""
  with mock.patch.object(
      main, 'run_pipeline', autospec=True, return_value={'status': 'DONE'}
  ) as mock_run:
    payload, code = _post(body)

  assert code == 200
  assert payload == {'status': 'DONE'}
  assert mock_run.call_args.kwargs['project'] == _PROJECT
  assert mock_run.call_args.kwargs['dataset'] == _DATASET


def test_http_reports_every_sheet_problem():
  """Verifies Sheet problems fail the run with the full list attached."""
  error = config_lib.SheetConfigError(['First problem.', 'Second problem.'])
  with mock.patch.object(
      main, 'run_pipeline', autospec=True, side_effect=error
  ):
    payload, code = _post(b'{}')

  assert code == 500
  assert payload == {
      'status': 'error',
      'reason': str(error),
      'problems': ['First problem.', 'Second problem.'],
  }


def test_http_reports_unexpected_errors():
  """Verifies any other failure is returned as a 500 with its message."""
  with mock.patch.object(
      main, 'run_pipeline', autospec=True, side_effect=RuntimeError('boom')
  ):
    payload, code = _post(b'{}')

  assert code == 500
  assert payload == {'status': 'error', 'reason': 'boom'}
