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

"""Unit tests for the keyword matcher entry point.

BigQuery is replaced by a fake that dispatches on the shape of the statement,
so these tests exercise the orchestration -- ordering, cleanup, the decision to
merge, and the HTTP contract -- without touching a real dataset.
"""

import json
from typing import Any, Optional
from unittest import mock

from keyword_matcher import main
import pytest

_KEYWORDS = [f'kw{index:03d}' for index in range(20)]
_HEALTHY_COUNTS = {keyword: 100 for keyword in _KEYWORDS}


class _FakeQueryJob:
  """Stands in for a BigQuery QueryJob."""

  def __init__(self, rows: list[dict[str, Any]]):
    self._rows = rows

  def result(self) -> list[dict[str, Any]]:
    return self._rows


class _FakeBigQueryClient:
  """A BigQuery client that answers from fixtures and records what it ran."""

  def __init__(
      self,
      keywords: Optional[list[str]] = None,
      previous_counts: Optional[dict[str, int]] = None,
      staging_counts: Optional[dict[str, int]] = None,
      raise_on_staging: bool = False,
  ):
    self.keywords = _KEYWORDS if keywords is None else keywords
    self.previous_counts = (
        dict(_HEALTHY_COUNTS) if previous_counts is None else previous_counts
    )
    self.staging_counts = (
        dict(_HEALTHY_COUNTS) if staging_counts is None else staging_counts
    )
    self.raise_on_staging = raise_on_staging
    self.statements: list[str] = []
    self.deleted_tables: list[str] = []

  def query(self, statement: str, job_config=None) -> _FakeQueryJob:
    del job_config  # The fake does not bind parameters.
    self.statements.append(statement)

    if statement.startswith('SELECT keyword FROM'):
      return _FakeQueryJob([{'keyword': word} for word in self.keywords])

    if statement.startswith('CREATE OR REPLACE TABLE'):
      if self.raise_on_staging:
        raise RuntimeError('staging failed')
      return _FakeQueryJob([])

    if statement.startswith('SELECT label, COUNT(*)'):
      counts = (
          self.previous_counts
          if '.detection`' in statement
          else self.staging_counts
      )
      return _FakeQueryJob([
          {'label': label, 'detections': detections}
          for label, detections in counts.items()
      ])

    if statement.startswith('MERGE'):
      return _FakeQueryJob([])

    raise AssertionError(f'Unexpected statement: {statement[:80]}')

  def delete_table(self, table: str, not_found_ok: bool = False) -> None:
    del not_found_ok
    self.deleted_tables.append(table)

  @property
  def merges(self) -> list[str]:
    return [item for item in self.statements if item.startswith('MERGE')]


class _FakeRequest:
  """Stands in for a flask request."""

  def __init__(self, payload: Any):
    self._payload = payload

  def get_json(self, silent: bool = False) -> Any:
    del silent
    return self._payload


def _patch_client(client: _FakeBigQueryClient):
  """Patches authentication and the BigQuery client constructor."""
  return (
      mock.patch.object(
          main.google.auth, 'default', return_value=(mock.Mock(), 'project')
      ),
      mock.patch.object(main.bigquery, 'Client', return_value=client),
  )


def _run(client: _FakeBigQueryClient, **kwargs) -> dict[str, Any]:
  auth_patch, client_patch = _patch_client(client)
  with auth_patch, client_patch:
    return main.run(**kwargs)


def _autospec_telemetry():
  """Returns a telemetry double that enforces the real log_step signature.

  create_autospec rather than a bare Mock is deliberate: it makes a mistyped
  or removed log_step keyword fail here, instead of surviving the suite and
  raising inside a deployed function where nothing is watching.
  """
  return mock.create_autospec(main.PipelineTelemetryContext, instance=True)


def _steps(telemetry) -> list[str]:
  """Returns the step names emitted, in order."""
  return [call.kwargs['step'] for call in telemetry.log_step.call_args_list]


def _failed_steps(telemetry) -> list[str]:
  """Returns the step names emitted with a FAILED status, in order."""
  return [
      call.kwargs['step']
      for call in telemetry.log_step.call_args_list
      if call.kwargs.get('status') == 'FAILED'
  ]


def test_drive_scope_is_requested():
  """exclusion_keywords is Drive-backed; without this scope the read fails."""
  assert 'https://www.googleapis.com/auth/drive' in main.SCOPES

  client = _FakeBigQueryClient()
  auth_patch, client_patch = _patch_client(client)
  with auth_patch as auth_default, client_patch:
    main.run()
  auth_default.assert_called_once_with(scopes=main.SCOPES)


def test_healthy_run_merges_both_entity_types():
  client = _FakeBigQueryClient()
  summary = _run(client)

  assert len(client.merges) == 2
  assert [result['entity_type'] for result in summary['results']] == [
      'video',
      'channel',
  ]
  assert all(result['committed'] for result in summary['results'])


def test_staging_tables_are_always_cleaned_up():
  client = _FakeBigQueryClient()
  _run(client)
  assert len(client.deleted_tables) == 2


def test_staging_table_is_cleaned_up_when_a_query_fails():
  client = _FakeBigQueryClient(raise_on_staging=True)
  auth_patch, client_patch = _patch_client(client)
  with auth_patch, client_patch:
    with pytest.raises(RuntimeError):
      main.run()
  assert len(client.deleted_tables) == 1


def test_refused_run_does_not_merge():
  """An empty keyword sheet must leave the previous detections alone.

  The refusal happens before any query is built, so no staging table is
  created and there is nothing to clean up. Only the keyword read runs.
  """
  client = _FakeBigQueryClient(keywords=[], staging_counts={})
  summary = _run(client)

  assert not client.merges
  assert not any(result['committed'] for result in summary['results'])
  assert all(
      result['abort_reason'] == 'no_usable_keywords'
      for result in summary['results']
  )
  assert len(client.statements) == 1
  assert client.statements[0].startswith('SELECT keyword FROM')
  assert not client.deleted_tables


def test_truncated_keyword_read_is_refused():
  client = _FakeBigQueryClient(
      keywords=_KEYWORDS[:2],
      previous_counts=dict(_HEALTHY_COUNTS),
      staging_counts={keyword: 100 for keyword in _KEYWORDS[:2]},
  )
  summary = _run(client)

  assert not client.merges
  assert all(
      result['abort_reason'] == 'keyword_set_retention_below_threshold'
      for result in summary['results']
  )


def test_acknowledgement_is_passed_through_to_the_breaker():
  client = _FakeBigQueryClient(
      keywords=_KEYWORDS[:2],
      previous_counts=dict(_HEALTHY_COUNTS),
      staging_counts={keyword: 100 for keyword in _KEYWORDS[:2]},
  )
  summary = _run(client, ack_keyword_set_change=True)

  assert len(client.merges) == 2
  assert all(result['committed'] for result in summary['results'])


def test_keyword_fingerprint_is_reported():
  client = _FakeBigQueryClient()
  summary = _run(client)
  assert len(summary['keyword_fingerprint']) == 64
  assert summary['keywords_accepted'] == len(_KEYWORDS)
  assert summary['detector_version'] == main.DETECTOR_VERSION


def test_http_success_returns_200():
  client = _FakeBigQueryClient()
  auth_patch, client_patch = _patch_client(client)
  with auth_patch, client_patch:
    response = main.main(_FakeRequest({}))
  assert response.status_code == 200
  assert json.loads(response.get_data(as_text=True))['status'] == 'Success'


def test_http_refusal_returns_500():
  """A refused run must return HTTP 500 so Cloud Scheduler marks the job failed."""
  client = _FakeBigQueryClient(keywords=[], staging_counts={})
  auth_patch, client_patch = _patch_client(client)
  with auth_patch, client_patch:
    response = main.main(_FakeRequest({}))
  assert response.status_code == 500
  body = json.loads(response.get_data(as_text=True))
  assert body['status'] == 'Failed'
  assert 'video' in body['message']


def test_http_rejects_a_malformed_payload():
  auth_patch, client_patch = _patch_client(_FakeBigQueryClient())
  with auth_patch, client_patch:
    response = main.main(
        _FakeRequest({'ack_keyword_set_change': 'not-a-boolean'})
    )
  assert response.status_code == 400


def test_http_accepts_an_absent_payload():
  """Cloud Scheduler may post an empty body."""
  client = _FakeBigQueryClient()
  auth_patch, client_patch = _patch_client(client)
  with auth_patch, client_patch:
    response = main.main(_FakeRequest(None))
  assert response.status_code == 200


def test_healthy_run_emits_a_step_for_every_stage():
  """Every stage boundary must be visible to the telemetry sink.

  The sink filters on jsonPayload.event_type='pipeline_step', so a stage that
  emits no step is invisible in BigQuery however loudly it logs in text.
  """
  client = _FakeBigQueryClient()
  telemetry = _autospec_telemetry()
  _run(client, telemetry=telemetry)

  assert _steps(telemetry) == [
      'LOAD_KEYWORDS',
      'COMPILE_PATTERNS',
      'COMPUTE_STAGING',
      'MERGE_DETECTIONS',
      'COMPUTE_STAGING',
      'MERGE_DETECTIONS',
      'COMPLETE',
  ]
  assert not _failed_steps(telemetry)


def test_merge_steps_report_the_detection_counts():
  client = _FakeBigQueryClient()
  telemetry = _autospec_telemetry()
  _run(client, telemetry=telemetry)

  merges = [
      call
      for call in telemetry.log_step.call_args_list
      if call.kwargs['step'] == 'MERGE_DETECTIONS'
  ]
  expected = sum(_HEALTHY_COUNTS.values())
  assert [call.kwargs['records_out'] for call in merges] == [expected, expected]
  assert [call.kwargs['metadata']['entity_type'] for call in merges] == [
      'video',
      'channel',
  ]


def test_empty_keyword_set_is_recorded_as_a_failed_step():
  client = _FakeBigQueryClient(keywords=[], staging_counts={})
  telemetry = _autospec_telemetry()
  _run(client, telemetry=telemetry)

  assert _failed_steps(telemetry) == ['KEYWORD_SET_EMPTY']
  assert 'MERGE_DETECTIONS' not in _steps(telemetry)


def test_breaker_refusal_is_recorded_as_a_failed_step():
  """A refusal is FAILED, not SKIPPED.

  SKIPPED means there was legitimately nothing to do. A refusal means work was
  computed and then discarded to protect the existing detections, which is an
  operator-visible event.
  """
  client = _FakeBigQueryClient(
      keywords=_KEYWORDS[:2],
      previous_counts=dict(_HEALTHY_COUNTS),
      staging_counts={keyword: 100 for keyword in _KEYWORDS[:2]},
  )
  telemetry = _autospec_telemetry()
  _run(client, telemetry=telemetry)

  assert _failed_steps(telemetry) == ['BREAKER_REFUSED', 'BREAKER_REFUSED']
  refusals = [
      call
      for call in telemetry.log_step.call_args_list
      if call.kwargs['step'] == 'BREAKER_REFUSED'
  ]
  assert all(
      call.kwargs['metadata']['reason']
      == 'keyword_set_retention_below_threshold'
      for call in refusals
  )


def test_entry_point_threads_one_context_through_the_whole_run():
  """One invocation must produce one run_id across every step."""
  client = _FakeBigQueryClient()
  telemetry = _autospec_telemetry()
  auth_patch, client_patch = _patch_client(client)
  with (
      auth_patch,
      client_patch,
      mock.patch.object(
          main, 'PipelineTelemetryContext', return_value=telemetry
      ),
  ):
    response = main.main(_FakeRequest({}))

  assert response.status_code == 200
  assert _steps(telemetry)[0] == 'LOAD_KEYWORDS'
  assert _steps(telemetry)[-1] == 'COMPLETE'


def test_unexpected_exception_is_recorded_as_a_failed_step():
  """An exception must not escape the handler unrecorded.

  If run() raises an unhandled exception, the HTTP handler must still emit a
  structured FAILED step before returning 500 so the failure is visible to the
  telemetry sink.
  """
  telemetry = _autospec_telemetry()
  with (
      mock.patch.object(
          main, 'PipelineTelemetryContext', return_value=telemetry
      ),
      mock.patch.object(main, 'run', side_effect=RuntimeError('boom')),
  ):
    response = main.main(_FakeRequest({}))

  assert response.status_code == 500
  assert _steps(telemetry) == ['ERROR']
  error_call = telemetry.log_step.call_args_list[0]
  assert error_call.kwargs['status'] == 'FAILED'
  assert isinstance(error_call.kwargs['error'], RuntimeError)


def test_malformed_payload_is_recorded_as_a_failed_step():
  telemetry = _autospec_telemetry()
  auth_patch, client_patch = _patch_client(_FakeBigQueryClient())
  with (
      auth_patch,
      client_patch,
      mock.patch.object(
          main, 'PipelineTelemetryContext', return_value=telemetry
      ),
  ):
    response = main.main(
        _FakeRequest({'ack_keyword_set_change': 'not-a-boolean'})
    )

  assert response.status_code == 400
  assert _failed_steps(telemetry) == ['VALIDATE_REQUEST']


def test_run_uses_configured_corpus_tables(monkeypatch):
  """Verifies that run() passes configured video and channel corpus table names to staging SQL."""
  monkeypatch.setattr(main, 'GOOGLE_CLOUD_PROJECT', 'test-project')
  monkeypatch.setattr(main, 'BQ_DATASET', 'video_exclusion_toolbox')
  monkeypatch.setattr(main, 'VIDEO_CORPUS_TABLE', 'custom_videos')
  monkeypatch.setattr(main, 'CHANNEL_CORPUS_TABLE', 'custom_channels')

  client = _FakeBigQueryClient()
  auth_patch, client_patch = _patch_client(client)
  with auth_patch, client_patch:
    main.run()

  staging_statements = [
      stmt
      for stmt in client.statements
      if stmt.startswith('CREATE OR REPLACE TABLE')
  ]
  assert len(staging_statements) == 2
  assert (
      '`test-project.video_exclusion_toolbox.custom_videos`'
      in staging_statements[0]
  )
  assert (
      '`test-project.video_exclusion_toolbox.custom_channels`'
      in staging_statements[1]
  )
