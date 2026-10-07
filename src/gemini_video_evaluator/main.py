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

"""Evaluates YouTube videos with Gemini, as set up in the Config Sheet.

Each run, triggered by Cloud Scheduler, works through four steps:

  1. Collect finished batches (`reconcile_batches`). The answers of Agent
     Platform Batch Inference jobs that have finished are parsed and merged
     into the detection table, and the jobs' temporary tables are dropped.
     This step does not read the Config Sheet: every output row carries the
     request it answered, which records what was asked, and the age threshold
     in force when it was sent. A broken Sheet therefore never holds up
     results that have already been paid for.
  2. Read the Config Sheet (`load_sheet_config`). If it has problems, the run
     stops here and reports every one of them.
  3. Sync the exclusion rules (`sync_detection_policies`). The detection_policy
     rows that belong to this evaluator are made to match the Sheet, so a
     changed mode or min_score applies at once to every video already scored.
  4. Send the next videos to Gemini (`discover_candidates`, thumbnail checks
     and a new batch job), unless sending is switched off in the Sheet or a
     batch is still running.

Run from the command line with `--blocking` to wait for the batch and collect
it in the same run, which is useful for testing.
"""

import argparse
import dataclasses
import datetime
import io
import json
import os
import sys
import time
from typing import Any, Optional
import uuid

import flask
import functions_framework
from gemini_video_evaluator.utils import batch as batch_lib
from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import prompt
from gemini_video_evaluator.utils import sql
from gemini_video_evaluator.utils import thumbnails
from google.api_core import exceptions as api_exceptions
import google.auth
from google.cloud import bigquery
import jsonschema
from vet_common import logging as vet_logging

PipelineTelemetryContext = vet_logging.PipelineTelemetryContext
logger = vet_logging.get_service_logger()

SERVICE_NAME = os.environ.get('K_SERVICE', 'vet-gemini-video-evaluator')

# Configuration from environment variables, set by Terraform.
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
BQ_DATASET = os.environ.get('VET_BIGQUERY_DATASET', 'video_exclusion_toolbox')
VIDEO_CORPUS_TABLE = os.environ.get('VET_VIDEO_CORPUS_TABLE', 'youtube_video')
DETECTION_TABLE = os.environ.get('VET_DETECTION_TABLE', 'detection')
DETECTION_POLICY_TABLE = os.environ.get(
    'VET_DETECTION_POLICY_TABLE', 'detection_policy'
)
VIDEOS_TO_EXCLUDE_VIEW = os.environ.get(
    'VET_VIDEOS_TO_EXCLUDE_VIEW', 'VideosToExclude'
)
GEMINI_SETTINGS_TABLE = os.environ.get(
    'VET_GEMINI_SETTINGS_TABLE', config_lib.SETTINGS_TAB
)
GEMINI_CATEGORIES_TABLE = os.environ.get(
    'VET_GEMINI_CATEGORIES_TABLE', config_lib.CATEGORIES_TAB
)
GEMINI_MODEL = os.environ.get('VET_GEMINI_MODEL', 'gemini-3.8-flash')
GEMINI_LOCATION = os.environ.get('VET_GEMINI_LOCATION', 'global')
# The most videos one batch may hold. The Config Sheet can lower it but not
# raise it, so a typo in the Sheet cannot multiply the cost of a run.
MAX_VIDEOS_PER_BATCH = int(os.environ.get('VET_MAX_VIDEOS_PER_BATCH', '10000'))

# A video counts as evaluated only once its batch has been collected. A second
# batch sent while the first is still running would therefore pick the same
# videos and pay for them twice.
MAX_CONCURRENT_JOBS = 1

# 'hq' (480x360) images exist for all four frame slots of a live video, and
# gave the same age and category accuracy as 'maxres' (1280x720) at a lower
# token cost. A single missing image fails the whole request row
# (URL_ERROR-ERROR_NOT_FOUND), so every URL is checked before it is attached,
# and an image counts as present only if it is larger than the 1,073-byte grey
# placeholder YouTube serves in place of a missing thumbnail.
THUMBNAIL_RESOLUTION = 'hq'
THUMBNAIL_SLOTS = ('default', '1', '2', '3')
MIN_THUMBNAIL_BYTES = 1500
PROBE_CONCURRENCY = 32
PROBE_TIMEOUT_SECONDS = 5.0

# The Drive scope is required, not optional: the two Gemini tabs are read
# through external tables backed by the Config Sheet, and a service account
# credential carries no Drive authority by default. Without it, reading them
# fails with "Permission denied while getting Drive credentials".
SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/drive',
]

# Every setting is read from the Config Sheet, so the request takes none. A
# parameter left over from an earlier version is rejected rather than ignored,
# so that nobody is led to believe it still has an effect.
REQUEST_SCHEMA = {
    'type': 'object',
    'properties': {},
    'additionalProperties': False,
}
_NO_PARAMETERS_HINT = (
    'The evaluator takes no request parameters: every setting is read from'
    f' the {config_lib.SETTINGS_TAB} and {config_lib.CATEGORIES_TAB} tabs of'
    ' the Config Sheet. Send an empty JSON object, {}.'
)

# Batch Inference copies every input column into the output table, which is
# how the threshold in force when a video was sent reaches the step that
# collects its answer.
BATCH_INPUT_SCHEMA = [
    bigquery.SchemaField(name, kind, mode='REQUIRED')
    for name, kind in (
        ('video_id', 'STRING'),
        ('run_id', 'STRING'),
        ('detector_version', 'STRING'),
        ('max_minor_age', 'INTEGER'),
        ('verified_thumbnail_count', 'INTEGER'),
        ('request', 'STRING'),
    )
]

DETECTION_STAGING_SCHEMA = [
    bigquery.SchemaField(name, kind, mode='REQUIRED')
    for name, kind in (
        ('entity_type', 'STRING'),
        ('entity_id', 'STRING'),
        ('modality', 'STRING'),
        ('source', 'STRING'),
        ('detector', 'STRING'),
        ('detector_version', 'STRING'),
        ('taxonomy', 'STRING'),
        ('label', 'STRING'),
        ('score', 'FLOAT'),
        ('evidence', 'STRING'),
    )
]

_TEMP_TABLE_EXPIRY_DAYS = 7


def _get_bigquery_client(project: str) -> bigquery.Client:
  """Builds a BigQuery client able to read Sheet-backed external tables."""
  credentials, _ = google.auth.default(scopes=SCOPES)
  return bigquery.Client(project=project, credentials=credentials)


def _get_genai_client(project: str, location: str) -> Any:
  """Builds an Agent Platform `google.genai.Client`."""
  from google import genai  # pylint: disable=g-import-not-at-top

  return genai.Client(vertexai=True, project=project, location=location)


def _table_exists(
    bq_client: bigquery.Client, project: str, dataset: str, table: str
) -> bool:
  """Returns True if `{project}.{dataset}.{table}` currently exists."""
  try:
    bq_client.get_table(f'{project}.{dataset}.{table}')
    return True
  except api_exceptions.NotFound:
    return False


def _drop_tables_quietly(
    bq_client: bigquery.Client, project: str, dataset: str, tables: list[str]
) -> None:
  """Drops temporary BigQuery tables with `not_found_ok=True`."""
  for name in tables:
    if name:
      bq_client.delete_table(f'{project}.{dataset}.{name}', not_found_ok=True)


def _load_ndjson_rows(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    rows: list[dict[str, Any]],
    schema: list[bigquery.SchemaField],
) -> None:
  """Loads `rows` into `{project}.{dataset}.{table}` via in-flight NDJSON."""
  table_ref = f'{project}.{dataset}.{table}'
  payload = '\n'.join(
      json.dumps(row, ensure_ascii=False) for row in rows
  ).encode('utf-8')
  job_config = bigquery.LoadJobConfig(
      schema=schema,
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
  )
  bq_client.load_table_from_file(
      io.BytesIO(payload), table_ref, job_config=job_config
  ).result()
  tbl = bq_client.get_table(table_ref)
  tbl.expires = datetime.datetime.now(
      datetime.timezone.utc
  ) + datetime.timedelta(days=_TEMP_TABLE_EXPIRY_DAYS)
  bq_client.update_table(tbl, ['expires'])


def _collect_job(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    detection_table: str,
    job: Any,
    output_table: str,
) -> dict[str, int]:
  """Merges the answers of one succeeded batch job into `detection`."""
  job_name = str(job.name)
  model = batch_lib.model_id(job)
  _, staging_table = batch_lib.derive_companion_tables(output_table)
  # SELECT * rather than a column list, because batches sent by earlier
  # versions have no max_minor_age column.
  rows = bq_client.query(
      f'SELECT * FROM {sql.qualified_table(project, dataset, output_table)}'
  ).result()

  records: list[batch_lib.DetectionRecord] = []
  failed: list[batch_lib.ParsedVideoEvaluation] = []
  succeeded = 0
  for row in rows:
    ev = batch_lib.parse_batch_output_row(dict(row), model)
    if ev.succeeded:
      succeeded += 1
      records.extend(ev.detections)
    else:
      logger.warning(
          'Batch job %s: the answer for video %s cannot be used: %s',
          job_name,
          ev.video_id or '(no video_id)',
          ev.error_message,
      )
      failed.append(ev)

  # A failed video is marked as evaluated (so it is not resent on every run)
  # only when at least one answer in the job succeeded; when none succeeded,
  # the cause is an outage or bad request rather than the videos themselves.
  recordable = [ev for ev in failed if ev.video_id]
  failures_recorded = 0
  if recordable and succeeded:
    records.extend(
        batch_lib.build_failure_audit_record(ev, model) for ev in recordable
    )
    failures_recorded = len(recordable)
  elif failed:
    logger.warning(
        'Batch job %s: none of its %d answers can be used, so its videos are'
        ' not marked as evaluated and will be sent again.',
        job_name,
        len(failed),
    )

  if records:
    _load_ndjson_rows(
        bq_client,
        project,
        dataset,
        staging_table,
        [dataclasses.asdict(r) for r in records],
        DETECTION_STAGING_SCHEMA,
    )
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    bq_client.query(
        sql.build_merge_detections_sql(
            project, dataset, staging_table, detection_table
        ),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('run_ts', 'TIMESTAMP', now_utc)
            ]
        ),
    ).result()
  return {
      'videos_reconciled': succeeded,
      'videos_failed': len(failed),
      'failures_recorded': failures_recorded,
      'detections_merged': len(records),
  }


def reconcile_batches(
    bq_client: bigquery.Client,
    genai_client: Any,
    project: str,
    dataset: str,
    detection_table: str = DETECTION_TABLE,
    max_scan_jobs: int = 50,
) -> dict[str, Any]:
  """Collects finished batch jobs and lists the ones still running."""
  # Uses `batches.list()` to find this evaluator's jobs, then `batches.get()`
  # for their live state because `list()` serves stale metadata. Succeeded jobs
  # are collected iff their output table still exists; dropping temp tables last
  # makes collection idempotent across retries.
  summary: dict[str, Any] = {
      'active_job_names': [],
      'reconciled_jobs': [],
      'cleaned_failed_jobs': [],
      'videos_reconciled': 0,
      'videos_failed': 0,
      'failures_recorded': 0,
      'detections_merged': 0,
  }
  scanned = 0
  for listed_job in genai_client.batches.list(config={'page_size': 20}):
    if scanned >= max_scan_jobs:
      break
    scanned += 1
    if not batch_lib.is_our_batch_job(listed_job, project, dataset):
      continue

    job = genai_client.batches.get(name=listed_job.name)
    job_name = str(job.name)
    state = batch_lib.normalize_job_state(job.state)
    destination = batch_lib.extract_output_bq_table(job)
    if destination is None:
      continue
    output_table = destination[2]
    input_table, staging_table = batch_lib.derive_companion_tables(output_table)
    temp_tables = [input_table, output_table, staging_table]

    if state in batch_lib.ACTIVE_JOB_STATES:
      summary['active_job_names'].append(job_name)
    elif state in batch_lib.FAILED_JOB_STATES:
      if _table_exists(bq_client, project, dataset, output_table) or (
          _table_exists(bq_client, project, dataset, input_table)
      ):
        logger.warning(
            'Batch job %s ended as %s (%s). Its videos will be sent again.',
            job_name,
            state,
            getattr(job, 'error', None) or 'no error details',
        )
        _drop_tables_quietly(bq_client, project, dataset, temp_tables)
        summary['cleaned_failed_jobs'].append(job_name)
    elif state in batch_lib.SUCCEEDED_JOB_STATES and _table_exists(
        bq_client, project, dataset, output_table
    ):
      counts = _collect_job(
          bq_client, project, dataset, detection_table, job, output_table
      )
      for key, value in counts.items():
        summary[key] += value
      _drop_tables_quietly(bq_client, project, dataset, temp_tables)
      summary['reconciled_jobs'].append(job_name)
  return summary


def _read_tab(
    bq_client: bigquery.Client, query: str, tab: str, problems: list[str]
) -> list[dict[str, Any]]:
  """Reads one Config Sheet tab, appending to `problems` on failure."""
  try:
    return [dict(row) for row in bq_client.query(query).result()]
  except api_exceptions.ClientError as exc:
    problems.append(
        f'The {tab} tab could not be read ({exc.message}). Check that the'
        f" Config Sheet has a tab named exactly '{tab}', and that the"
        " tool's service account can view the Sheet."
    )
    return []


def load_sheet_config(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    max_videos_ceiling: int = MAX_VIDEOS_PER_BATCH,
    now: Optional[datetime.datetime] = None,
) -> tuple[config_lib.SheetConfig, list[str]]:
  """Reads and validates the two Gemini tabs of the Config Sheet."""
  problems: list[str] = []
  settings_rows = _read_tab(
      bq_client,
      sql.build_read_settings_sql(project, dataset, GEMINI_SETTINGS_TABLE),
      config_lib.SETTINGS_TAB,
      problems,
  )
  category_rows = _read_tab(
      bq_client,
      sql.build_read_categories_sql(project, dataset, GEMINI_CATEGORIES_TABLE),
      config_lib.CATEGORIES_TAB,
      problems,
  )
  if problems:
    raise config_lib.SheetConfigError(problems)
  return config_lib.parse_sheet_config(
      settings_rows,
      category_rows,
      max_videos_ceiling=max_videos_ceiling,
      now=now or datetime.datetime.now(datetime.timezone.utc),
  )


def sync_detection_policies(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    sheet_config: config_lib.SheetConfig,
) -> list[sql.PolicyRow]:
  """Makes this evaluator's detection_policy rows match the Config Sheet."""
  policy_rows = sql.build_policy_rows(sheet_config)
  # Never empty, because the age check always has a row: the client cannot
  # send an empty array of structs.
  struct_fields = (
      ('policy_id', 'STRING'),
      ('taxonomy_prefix', 'STRING'),
      ('min_score', 'FLOAT64'),
      ('action', 'STRING'),
      ('enabled', 'BOOL'),
      ('notes', 'STRING'),
  )
  policies = bigquery.ArrayQueryParameter(
      'policies',
      'STRUCT',
      [
          bigquery.StructQueryParameter(
              None,
              *(
                  bigquery.ScalarQueryParameter(name, kind, getattr(row, name))
                  for name, kind in struct_fields
              ),
          )
          for row in policy_rows
      ],
  )
  bq_client.query(
      sql.build_sync_detection_policy_sql(
          project, dataset, DETECTION_POLICY_TABLE
      ),
      job_config=bigquery.QueryJobConfig(query_parameters=[policies]),
  ).result()
  age_param = bigquery.ScalarQueryParameter(
      'max_minor_age', 'INT64', sheet_config.max_minor_age
  )
  bq_client.query(
      sql.build_recompute_age_scores_sql(project, dataset, DETECTION_TABLE),
      job_config=bigquery.QueryJobConfig(query_parameters=[age_param]),
  ).result()
  return policy_rows


def discover_candidates(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    max_videos: int,
    reevaluate_before: Optional[datetime.datetime],
) -> list[dict[str, Any]]:
  """Picks the next videos to send (see `sql.build_candidate_discovery_sql`)."""
  query = sql.build_candidate_discovery_sql(
      project,
      dataset,
      VIDEO_CORPUS_TABLE,
      DETECTION_TABLE,
      VIDEOS_TO_EXCLUDE_VIEW,
  )
  job_config = bigquery.QueryJobConfig(
      query_parameters=[
          bigquery.ScalarQueryParameter('max_videos', 'INT64', max_videos),
          bigquery.ScalarQueryParameter(
              'reevaluate_before', 'TIMESTAMP', reevaluate_before
          ),
      ]
  )
  rows = bq_client.query(query, job_config=job_config).result()
  return [dict(row) for row in rows]


def _send_batch(
    bq_client: bigquery.Client,
    genai_client: Any,
    project: str,
    dataset: str,
    sheet_config: config_lib.SheetConfig,
    candidates: list[dict[str, Any]],
    telemetry: PipelineTelemetryContext,
) -> dict[str, Any]:
  """Checks the candidates' thumbnails, stages the requests, starts a job."""
  video_ids = [str(c['video_id']).strip() for c in candidates]
  probes = thumbnails.probe_video_thumbnails(
      video_ids=video_ids,
      resolution=THUMBNAIL_RESOLUTION,
      slots=THUMBNAIL_SLOTS,
      min_bytes=MIN_THUMBNAIL_BYTES,
      concurrency=PROBE_CONCURRENCY,
      timeout_seconds=PROBE_TIMEOUT_SECONDS,
  )
  verified_urls = {
      vid: probes[vid].verified_urls if vid in probes else ()
      for vid in video_ids
  }
  total_thumbnails = sum(len(urls) for urls in verified_urls.values())
  with_thumbnails = sum(1 for urls in verified_urls.values() if urls)
  telemetry.log_step(
      step='PROBE_THUMBNAILS',
      records_in=len(video_ids),
      records_out=total_thumbnails,
      metadata={
          'resolution': THUMBNAIL_RESOLUTION,
          'videos_with_thumbnails': with_thumbnails,
          'videos_without_thumbnails': len(video_ids) - with_thumbnails,
      },
  )

  now_tag = datetime.datetime.now(datetime.timezone.utc).strftime(
      '%Y%m%d_%H%M%S'
  )
  run_tag = f'{now_tag}_{uuid.uuid4().hex[:8]}'
  input_table = f'{batch_lib.INPUT_TABLE_PREFIX}{run_tag}'
  output_table = f'{batch_lib.OUTPUT_TABLE_PREFIX}{run_tag}'
  input_rows = [
      batch_lib.build_batch_input_row(
          c, verified_urls[vid], sheet_config, telemetry.run_id
      )
      for c, vid in zip(candidates, video_ids)
  ]
  _load_ndjson_rows(
      bq_client, project, dataset, input_table, input_rows, BATCH_INPUT_SCHEMA
  )
  telemetry.log_step(
      step='STAGE_BATCH_INPUT',
      records_in=len(candidates),
      records_out=len(input_rows),
      metadata={'input_table': input_table, 'output_table': output_table},
  )

  display_name = f'{batch_lib.DISPLAY_NAME_PREFIX}{run_tag}'
  job = genai_client.batches.create(
      model=GEMINI_MODEL,
      src=f'bq://{project}.{dataset}.{input_table}',
      config={
          'display_name': display_name,
          'dest': f'bq://{project}.{dataset}.{output_table}',
      },
  )
  dispatched = {
      'job_name': str(job.name),
      'display_name': display_name,
      'model': GEMINI_MODEL,
      'detector_version': prompt.DETECTOR_VERSION,
      'state': batch_lib.normalize_job_state(job.state),
      'input_table': input_table,
      'output_table': output_table,
      'videos_submitted': len(input_rows),
      'total_verified_thumbnails': total_thumbnails,
  }
  telemetry.log_step(
      step='DISPATCH_BATCH_JOB',
      records_in=len(input_rows),
      records_out=1,
      metadata=dispatched,
  )
  return dispatched


def _wait_for_jobs(
    genai_client: Any, job_names: list[str], poll_interval_seconds: float
) -> dict[str, str]:
  """Polls until none of `job_names` is active."""
  states: dict[str, str] = {}
  pending = list(job_names)
  while pending:
    time.sleep(poll_interval_seconds)
    still_active = []
    for job_name in pending:
      job = genai_client.batches.get(name=job_name)
      state = batch_lib.normalize_job_state(job.state)
      states[job_name] = state
      logger.info(
          'Batch job %s: %s, %s',
          job_name.rsplit('/', 1)[-1],
          state,
          batch_lib.format_completion_stats(job),
      )
      if state in batch_lib.ACTIVE_JOB_STATES:
        still_active.append(job_name)
    pending = still_active
  return states


def _complete(
    telemetry: PipelineTelemetryContext,
    summary: dict[str, Any],
    records_in: int = 0,
    records_out: int = 0,
) -> dict[str, Any]:
  """Logs the final step of a run and returns its summary."""
  telemetry.log_step(
      step='COMPLETE',
      records_in=records_in,
      records_out=records_out,
      metadata=summary,
  )
  return summary


def run_pipeline(
    project: str,
    dataset: str,
    telemetry: PipelineTelemetryContext,
    blocking: bool = False,
    poll_interval_seconds: float = 15.0,
    max_videos_cap: Optional[int] = None,
    bq_client: Optional[bigquery.Client] = None,
    genai_client: Optional[Any] = None,
) -> dict[str, Any]:
  """Runs one cycle: collect, read the Sheet, sync the rules, send videos."""
  bq = bq_client or _get_bigquery_client(project)
  ai = genai_client or _get_genai_client(project, GEMINI_LOCATION)

  # 1. Collect finished batches before reading the Sheet, so a broken Sheet
  # never holds up results that have already been paid for.
  reconcile = reconcile_batches(bq, ai, project, dataset)
  telemetry.log_step(
      step='RECONCILE_BATCHES',
      records_in=len(reconcile['reconciled_jobs']),
      records_out=reconcile['detections_merged'],
      metadata=reconcile,
  )

  # 2. Read the Config Sheet.
  try:
    sheet_config, warnings = load_sheet_config(bq, project, dataset)
  except config_lib.SheetConfigError as exc:
    telemetry.log_step(
        step='READ_CONFIG',
        status='FAILED',
        error=exc,
        metadata={'problems': list(exc.problems)},
    )
    raise
  for warning in warnings:
    logger.warning('%s', warning)
  reeval_iso = (
      sheet_config.reevaluate_before.isoformat()
      if sheet_config.reevaluate_before
      else None
  )
  telemetry.log_step(
      step='READ_CONFIG',
      records_out=len(sheet_config.categories),
      metadata={
          'age_check_mode': sheet_config.age_check_mode,
          'max_minor_age': sheet_config.max_minor_age,
          'advertiser_context_set': bool(sheet_config.advertiser_context),
          'max_videos_per_batch': sheet_config.max_videos_per_batch,
          'reevaluate_before': reeval_iso,
          'categories': [
              {'name': c.name, 'mode': c.mode, 'min_score': c.min_score}
              for c in sheet_config.categories
          ],
          'warnings': warnings,
      },
  )

  # 3. Make the exclusion rules match the Sheet.
  policy_rows = sync_detection_policies(bq, project, dataset, sheet_config)
  telemetry.log_step(
      step='SYNC_POLICIES',
      records_out=len(policy_rows),
      metadata={
          'policies': [
              {
                  'policy_id': r.policy_id,
                  'action': r.action,
                  'enabled': r.enabled,
                  'min_score': r.min_score,
              }
              for r in policy_rows
          ]
      },
  )

  # 4. Send the next videos, if sending is on and no batch is running.
  base = {
      'reconcile': reconcile,
      'warnings': warnings,
      'dispatched_job': None,
      'candidates_discovered': 0,
  }
  merged = reconcile['detections_merged']
  max_videos = sheet_config.max_videos_per_batch
  if max_videos_cap is not None:
    max_videos = min(max_videos, max_videos_cap)

  disabled_reason = None
  if not sheet_config.anything_active:
    disabled_reason = (
        'The age check and every category are Disabled in the Config Sheet,'
        ' so there is nothing to ask Gemini and no new videos are sent.'
    )
  elif max_videos == 0:
    disabled_reason = (
        f'max_videos_per_batch is 0 in the {config_lib.SETTINGS_TAB} tab, so'
        ' no new videos are sent.'
    )
  if disabled_reason:
    logger.info('%s', disabled_reason)
    return _complete(
        telemetry,
        {'status': 'DISPATCH_DISABLED', 'reason': disabled_reason, **base},
        records_out=merged,
    )

  active_jobs = reconcile['active_job_names']
  if len(active_jobs) >= MAX_CONCURRENT_JOBS:
    if not blocking:
      return _complete(
          telemetry,
          {'status': 'IN_FLIGHT_THROTTLED', **base},
          records_out=merged,
      )
    final_states = _wait_for_jobs(ai, active_jobs, poll_interval_seconds)
    blocking_reconcile = reconcile_batches(bq, ai, project, dataset)
    telemetry.log_step(
        step='WAIT_FOR_BATCH',
        records_in=len(active_jobs),
        records_out=blocking_reconcile['detections_merged'],
        metadata={
            'final_states': final_states,
            'reconcile': blocking_reconcile,
        },
    )
    return _complete(
        telemetry,
        {
            'status': 'RECONCILED_IN_FLIGHT',
            **base,
            'blocking_reconcile': blocking_reconcile,
        },
        records_out=merged + blocking_reconcile['detections_merged'],
    )

  candidates = discover_candidates(
      bq, project, dataset, max_videos, sheet_config.reevaluate_before
  )
  reevaluations = sum(1 for c in candidates if c.get('reevaluation'))
  telemetry.log_step(
      step='DISCOVER_CANDIDATES',
      records_in=max_videos,
      records_out=len(candidates),
      metadata={
          'new_videos': len(candidates) - reevaluations,
          'reevaluations': reevaluations,
          'reevaluate_before': reeval_iso,
      },
  )
  if not candidates:
    return _complete(
        telemetry, {'status': 'NO_CANDIDATES', **base}, records_out=merged
    )

  dispatched = _send_batch(
      bq, ai, project, dataset, sheet_config, candidates, telemetry
  )
  summary = {
      **base,
      'dispatched_job': dispatched,
      'candidates_discovered': len(candidates),
  }
  if not blocking:
    return _complete(
        telemetry,
        {'status': 'DISPATCHED', **summary},
        records_in=len(candidates),
        records_out=merged,
    )

  job_name = dispatched['job_name']
  if dispatched['state'] in batch_lib.ACTIVE_JOB_STATES:
    dispatched['state'] = _wait_for_jobs(ai, [job_name], poll_interval_seconds)[
        job_name
    ]
  blocking_reconcile = reconcile_batches(bq, ai, project, dataset)
  telemetry.log_step(
      step='WAIT_FOR_BATCH',
      status=(
          'SUCCESS'
          if dispatched['state'] in batch_lib.SUCCEEDED_JOB_STATES
          else 'FAILED'
      ),
      records_in=len(candidates),
      records_out=blocking_reconcile['detections_merged'],
      metadata={
          'job_name': job_name,
          'terminal_state': dispatched['state'],
          'reconcile': blocking_reconcile,
      },
  )
  return _complete(
      telemetry,
      {
          'status': 'COMPLETED_BLOCKING',
          **summary,
          'blocking_reconcile': blocking_reconcile,
      },
      records_in=len(candidates),
      records_out=merged + blocking_reconcile['detections_merged'],
  )


def _bad_request(
    telemetry: PipelineTelemetryContext, reason: str
) -> tuple[flask.Response, int]:
  """Logs and returns a 400 response."""
  telemetry.log_step(
      step='VALIDATE_REQUEST', status='FAILED', error=ValueError(reason)
  )
  return flask.jsonify({'status': 'error', 'reason': reason}), 400


@functions_framework.http
def main(request: flask.Request) -> tuple[flask.Response, int]:
  """The HTTP entry point called by Cloud Scheduler."""
  telemetry = PipelineTelemetryContext(logger=logger, service_name=SERVICE_NAME)
  payload: Any = {}
  if request.data:
    payload = request.get_json(silent=True)
    if payload is None:
      return _bad_request(
          telemetry,
          f'The request body is not valid JSON. {_NO_PARAMETERS_HINT}',
      )
  try:
    jsonschema.validate(instance=payload, schema=REQUEST_SCHEMA)
  except jsonschema.ValidationError as exc:
    return _bad_request(telemetry, f'{exc.message}. {_NO_PARAMETERS_HINT}')

  try:
    if not GOOGLE_CLOUD_PROJECT:
      raise ValueError('GOOGLE_CLOUD_PROJECT must be set.')
    summary = run_pipeline(
        project=GOOGLE_CLOUD_PROJECT, dataset=BQ_DATASET, telemetry=telemetry
    )
  except config_lib.SheetConfigError as exc:
    # Already logged, with every problem, as the failed READ_CONFIG step.
    return (
        flask.jsonify({
            'status': 'error',
            'reason': str(exc),
            'problems': list(exc.problems),
        }),
        500,
    )
  except Exception as exc:  # pylint: disable=broad-exception-caught
    logger.exception('Gemini video evaluator run failed.')
    telemetry.log_step(step='ERROR', status='FAILED', error=exc)
    return flask.jsonify({'status': 'error', 'reason': str(exc)}), 500
  return flask.jsonify(summary), 200


def _cli() -> None:
  """Runs one cycle from the command line."""
  parser = argparse.ArgumentParser(
      description='Run the VET Gemini video evaluator once.'
  )
  parser.add_argument(
      '--project',
      default=GOOGLE_CLOUD_PROJECT,
      help='Google Cloud project ID. Defaults to GOOGLE_CLOUD_PROJECT.',
  )
  parser.add_argument(
      '--dataset', default=BQ_DATASET, help='BigQuery dataset ID.'
  )
  parser.add_argument(
      '--blocking',
      action='store_true',
      help='Wait for the batch to finish and collect it in the same run.',
  )
  parser.add_argument(
      '--poll-interval',
      type=float,
      default=15.0,
      help='Seconds between status checks with --blocking.',
  )
  parser.add_argument(
      '--max-videos',
      type=int,
      default=None,
      help=(
          'Send at most this many videos, if the Config Sheet allows more.'
          ' For test runs.'
      ),
  )
  args = parser.parse_args()
  if not args.project:
    parser.error('--project is required when GOOGLE_CLOUD_PROJECT is not set.')
  if args.max_videos is not None and args.max_videos < 1:
    parser.error('--max-videos must be at least 1.')
  if args.poll_interval <= 0:
    parser.error('--poll-interval must be greater than 0.')

  telemetry = PipelineTelemetryContext(logger=logger, service_name=SERVICE_NAME)
  try:
    result = run_pipeline(
        project=args.project,
        dataset=args.dataset,
        telemetry=telemetry,
        blocking=args.blocking,
        poll_interval_seconds=args.poll_interval,
        max_videos_cap=args.max_videos,
    )
  except config_lib.SheetConfigError as exc:
    sys.exit(str(exc))
  print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
  _cli()
