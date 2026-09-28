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

"""Detects advertiser exclusion keywords in YouTube video and channel text.

The logic lives in the utils package and is pure; this module is the
orchestration around it: read the keywords, compute detections into a staging
table, ask the circuit breaker whether the result is credible, and merge.

Video and channel detections are evaluated and committed independently. They
occupy disjoint rows of the detection table, so an abort on one does not
require discarding the other, and the surviving entity type keeps its previous
detections either way.
"""

import datetime
import os
from typing import Optional
import uuid

import flask
import functions_framework
import google.auth
from google.cloud import bigquery
import jsonschema
from keyword_matcher.utils import breaker
from keyword_matcher.utils import patterns
from keyword_matcher.utils import sql
from vet_common.logging import PipelineTelemetryContext, get_service_logger

logger = get_service_logger()

# The telemetry context needs the same identity the logger derives internally.
# It is read here rather than passed around so that the fallback is stated once.
SERVICE_NAME = os.environ.get('K_SERVICE', 'vet-keyword-matcher')

# Configuration from environment variables
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
BQ_DATASET = os.environ.get('VET_BIGQUERY_DATASET')
VIDEO_CORPUS_TABLE = os.environ.get('VET_VIDEO_CORPUS_TABLE', 'youtube_video')
CHANNEL_CORPUS_TABLE = os.environ.get(
    'VET_CHANNEL_CORPUS_TABLE', 'youtube_channel'
)
KEYWORD_TABLE = os.environ.get('VET_KEYWORD_TABLE', 'exclusion_keywords')
DETECTION_TABLE = os.environ.get('VET_DETECTION_TABLE', 'detection')

# Hand-maintained. Bump this whenever pattern construction changes, so that
# rows written by different rules can be told apart after the fact.
DETECTOR_VERSION = '2026.09-v1'

MIN_RETENTION_RATIO = float(os.environ.get('VET_MIN_RETENTION_RATIO', '0.9'))
MIN_KEYWORD_RETENTION_RATIO = float(
    os.environ.get('VET_MIN_KEYWORD_RETENTION_RATIO', '0.9')
)

# The access scopes used in this function. The Drive scope is required and is
# not optional: exclusion_keywords is an external table backed by a Google
# Sheet, and a service account credential carries no Drive authority by
# default. Without it, reading the keywords fails with "Access Denied:
# Permission denied while getting Drive credentials".
SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/drive',
]

# The schema of the JSON in the event payload.
REQUEST_SCHEMA = {
    'type': 'object',
    'properties': {
        'ack_keyword_set_change': {'type': 'boolean'},
    },
    'additionalProperties': True,
}


def _get_bigquery_client() -> bigquery.Client:
  """Builds a BigQuery client able to read Drive-backed external tables.

  Returns:
      A BigQuery client.
  """
  credentials, _ = google.auth.default(scopes=SCOPES)
  return bigquery.Client(project=GOOGLE_CLOUD_PROJECT, credentials=credentials)


def _load_keywords(client: bigquery.Client) -> list[str]:
  """Reads the exclusion keywords from the sheet-backed external table.

  Args:
      client: The BigQuery client.

  Returns:
      Keywords exactly as they appear in the sheet.
  """
  table = sql.qualified_table(GOOGLE_CLOUD_PROJECT, BQ_DATASET, KEYWORD_TABLE)
  rows = client.query(f'SELECT keyword FROM {table}').result()
  keywords = [row['keyword'] for row in rows if row['keyword'] is not None]
  logger.info('Read %d keyword rows from %s.', len(keywords), table)
  return keywords


def _keyword_parameters(
    pattern_set: patterns.PatternSet,
) -> list[bigquery.ArrayQueryParameter]:
  """Turns the compiled patterns into BigQuery query parameters.

  Args:
      pattern_set: The compiled keyword patterns.

  Returns:
      The query parameters used by the staging statement.
  """
  structs = [
      bigquery.StructQueryParameter(
          None,
          bigquery.ScalarQueryParameter('keyword', 'STRING', item.keyword),
          bigquery.ScalarQueryParameter('pattern', 'STRING', item.pattern),
      )
      for item in pattern_set.patterns
  ]
  return [
      bigquery.ArrayQueryParameter('keyword_patterns', 'STRUCT', structs),
      bigquery.ScalarQueryParameter(
          'admission_pattern', 'STRING', pattern_set.admission_pattern
      ),
  ]


def _counts_by_label(client: bigquery.Client, statement: str) -> dict[str, int]:
  """Runs a label/detections query and returns it as a mapping.

  Args:
      client: The BigQuery client.
      statement: A query returning the columns label and detections.

  Returns:
      Detections keyed by label.
  """
  return {
      row['label']: row['detections']
      for row in client.query(statement).result()
  }


def _summarise(
    entity_type: str, decision: breaker.BreakerDecision
) -> dict[str, object]:
  """Renders a breaker decision as a reportable summary.

  Args:
      entity_type: Either 'video' or 'channel'.
      decision: The breaker's verdict.

  Returns:
      A JSON-serialisable summary.
  """
  return {
      'entity_type': entity_type,
      'committed': decision.proceed,
      'abort_reason': decision.reason,
      'seeding_run': decision.seeding_run,
      'warnings': decision.warnings,
      'metrics': decision.metrics._asdict(),
  }


def _process_entity_type(
    client: bigquery.Client,
    entity_type: str,
    pattern_set: patterns.PatternSet,
    run_ts: datetime.datetime,
    ack_keyword_set_change: bool,
    telemetry: PipelineTelemetryContext,
) -> dict[str, object]:
  """Computes, checks and applies detections for one entity type.

  Args:
      client: The BigQuery client.
      entity_type: Either 'video' or 'channel'.
      pattern_set: The compiled keyword patterns.
      run_ts: Timestamp recorded on every row this run touches.
      ack_keyword_set_change: Whether a deliberate bulk keyword edit has been
        acknowledged for this run.
      telemetry: Context used to emit a structured step per stage.

  Returns:
      A summary of what happened, suitable for the HTTP response.
  """
  staging_table = f'_keyword_match_staging_{entity_type}_{uuid.uuid4().hex[:8]}'
  source_table = (
      VIDEO_CORPUS_TABLE
      if entity_type == sql.ENTITY_VIDEO
      else CHANNEL_CORPUS_TABLE
  )
  logger.info('Computing %s detections into %s.', entity_type, staging_table)

  try:
    client.query(
        sql.build_staging_sql(
            GOOGLE_CLOUD_PROJECT,
            BQ_DATASET,
            staging_table,
            entity_type,
            source_table=source_table,
        ),
        job_config=bigquery.QueryJobConfig(
            query_parameters=_keyword_parameters(pattern_set)
        ),
    ).result()

    previous_counts = _counts_by_label(
        client,
        sql.build_previous_counts_sql(
            GOOGLE_CLOUD_PROJECT, BQ_DATASET, entity_type, DETECTION_TABLE
        ),
    )
    staging_counts = _counts_by_label(
        client,
        sql.build_staging_counts_sql(
            GOOGLE_CLOUD_PROJECT, BQ_DATASET, staging_table
        ),
    )

    telemetry.log_step(
        step='COMPUTE_STAGING',
        records_in=len(pattern_set.patterns),
        records_out=sum(staging_counts.values()),
        metadata={
            'entity_type': entity_type,
            'staging_table': staging_table,
            'labels_matched': len(staging_counts),
            'previous_labels': len(previous_counts),
        },
    )

    decision = breaker.evaluate(
        current_keywords=pattern_set.keywords,
        previous_counts=previous_counts,
        staging_counts=staging_counts,
        ack_keyword_set_change=ack_keyword_set_change,
        min_keyword_retention_ratio=MIN_KEYWORD_RETENTION_RATIO,
        min_retention_ratio=MIN_RETENTION_RATIO,
        logger=logger,
    )

    summary = _summarise(entity_type, decision)

    if not decision.proceed:
      # Nothing is written. The previous detections remain in place, which is
      # the entire purpose of merging rather than replacing.
      logger.error(
          'Refusing to apply %s detections: %s', entity_type, decision.reason
      )
      # Recorded as FAILED rather than SKIPPED: the reference services reserve
      # SKIPPED for "there was legitimately nothing to do", whereas a refusal
      # means work was discarded to protect the existing detections.
      telemetry.log_step(
          step='BREAKER_REFUSED',
          status='FAILED',
          records_in=decision.metrics.staging_rows,
          records_out=0,
          metadata={
              'entity_type': entity_type,
              'reason': decision.reason,
              'seeding_run': decision.seeding_run,
              'warnings': decision.warnings,
              **decision.metrics._asdict(),
          },
      )
      return summary

    client.query(
        sql.build_merge_sql(
            GOOGLE_CLOUD_PROJECT,
            BQ_DATASET,
            staging_table,
            entity_type,
            DETECTION_TABLE,
        ),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('run_ts', 'TIMESTAMP', run_ts),
                bigquery.ScalarQueryParameter(
                    'detector_version', 'STRING', DETECTOR_VERSION
                ),
            ]
        ),
    ).result()
    logger.info(
        'Applied %d %s detections.', decision.metrics.staging_rows, entity_type
    )
    telemetry.log_step(
        step='MERGE_DETECTIONS',
        records_in=decision.metrics.staging_rows,
        records_out=decision.metrics.staging_rows,
        metadata={
            'entity_type': entity_type,
            'seeding_run': decision.seeding_run,
            'warnings': decision.warnings,
            **decision.metrics._asdict(),
        },
    )
    return summary

  finally:
    client.delete_table(
        f'{GOOGLE_CLOUD_PROJECT}.{BQ_DATASET}.{staging_table}',
        not_found_ok=True,
    )


def run(
    ack_keyword_set_change: bool = False,
    telemetry: Optional[PipelineTelemetryContext] = None,
) -> dict[str, object]:
  """Executes one detection run across both entity types.

  Args:
      ack_keyword_set_change: Waives the keyword-set assertion for this run. Set
        only by a human triggering a manual run after a deliberate bulk edit to
        the keyword sheet.
      telemetry: Context used to emit a structured step per stage. Constructed
        if absent, so that run() stays callable on its own.

  Returns:
      A summary of the run.
  """
  telemetry = telemetry or PipelineTelemetryContext(
      logger=logger, service_name=SERVICE_NAME
  )
  run_ts = datetime.datetime.now(datetime.UTC)
  client = _get_bigquery_client()

  keywords = _load_keywords(client)
  telemetry.log_step(
      step='LOAD_KEYWORDS',
      records_out=len(keywords),
      metadata={'keyword_table': KEYWORD_TABLE},
  )

  pattern_set = patterns.build_pattern_set(keywords, logger)
  logger.info(
      'Keyword fingerprint %s (%d usable keywords).',
      pattern_set.fingerprint,
      len(pattern_set.patterns),
  )
  telemetry.log_step(
      step='COMPILE_PATTERNS',
      records_in=len(keywords),
      records_out=len(pattern_set.patterns),
      metadata={
          'keyword_fingerprint': pattern_set.fingerprint,
          'keywords_rejected': pattern_set.rejected,
          'detector_version': DETECTOR_VERSION,
      },
  )

  entity_types = (sql.ENTITY_VIDEO, sql.ENTITY_CHANNEL)

  # Refuse before touching BigQuery. This has to happen here rather than inside
  # the per-entity path: an empty keyword set cannot be expressed as a query
  # parameter at all, so building the staging query would raise before the
  # breaker was ever consulted, turning the single failure mode this detector
  # most needs to report cleanly into an opaque stack trace.
  if not pattern_set.patterns:
    decision = breaker.evaluate(
        current_keywords=[],
        previous_counts={},
        staging_counts={},
        logger=logger,
    )
    telemetry.log_step(
        step='KEYWORD_SET_EMPTY',
        status='FAILED',
        records_in=len(keywords),
        records_out=0,
        metadata={'reason': decision.reason},
    )
    results = [
        _summarise(entity_type, decision) for entity_type in entity_types
    ]
  else:
    results = [
        _process_entity_type(
            client,
            entity_type,
            pattern_set,
            run_ts,
            ack_keyword_set_change,
            telemetry,
        )
        for entity_type in entity_types
    ]

  committed = sum(
      result['metrics']['staging_rows']
      for result in results
      if result['committed']
  )
  telemetry.log_step(
      step='COMPLETE',
      records_in=len(pattern_set.patterns),
      records_out=committed,
      metadata={
          'entity_types_committed': [
              result['entity_type'] for result in results if result['committed']
          ],
          'entity_types_refused': [
              result['entity_type']
              for result in results
              if not result['committed']
          ],
      },
  )

  return {
      'detector_version': DETECTOR_VERSION,
      'keyword_fingerprint': pattern_set.fingerprint,
      'keywords_accepted': len(pattern_set.patterns),
      'keywords_rejected': pattern_set.rejected,
      'results': results,
  }


@functions_framework.http
def main(request: flask.Request) -> flask.Response:
  """The entry point: runs the keyword detector.

  Args:
      request (flask.Request): HTTP request object. The payload may carry
        ack_keyword_set_change.

  Returns:
      The flask response. A run in which any entity type was refused returns
      HTTP 500 so that Cloud Scheduler records the invocation as failed.
  """
  logger.info('Keyword detection started.')

  # Owned here rather than in run() so that every step of one invocation,
  # including failures that happen before run() is reached, shares a run_id.
  # There is no CloudEvent to take an id from -- the trigger is a Cloud
  # Scheduler HTTP POST -- so the context mints a uuid4, as the dispatcher does.
  telemetry = PipelineTelemetryContext(logger=logger, service_name=SERVICE_NAME)

  request_json = request.get_json(silent=True) or {}
  response: dict[str, object] = {}
  try:
    jsonschema.validate(instance=request_json, schema=REQUEST_SCHEMA)
  except jsonschema.exceptions.ValidationError as err:
    logger.error('Invalid request payload: %s', err)
    telemetry.log_step(
        step='VALIDATE_REQUEST',
        status='FAILED',
        error=err,
        metadata={'message': err.message},
    )
    response['status'] = 'Failed'
    response['message'] = err.message
    return flask.Response(
        flask.json.dumps(response), status=400, mimetype='application/json'
    )

  ack_keyword_set_change = bool(
      request_json.get('ack_keyword_set_change', False)
  )
  if ack_keyword_set_change:
    logger.warning(
        'Run carries an acknowledgement of a keyword set change; the '
        'keyword-set assertion is waived for this run only.'
    )

  try:
    summary = run(
        ack_keyword_set_change=ack_keyword_set_change, telemetry=telemetry
    )
  except Exception as e:  # pylint: disable=broad-exception-caught
    # Without this the exception escapes the handler: the platform still
    # returns 500, but no FAILED step is ever emitted, so the run is invisible
    # to everything downstream of the telemetry sink.
    logger.exception('Unexpected error executing keyword detection: %s', e)
    telemetry.log_step(
        step='ERROR',
        status='FAILED',
        error=e,
        metadata={'ack_keyword_set_change': ack_keyword_set_change},
    )
    response['status'] = 'Failed'
    response['message'] = f'Internal Server Error: {e}'
    return flask.Response(
        flask.json.dumps(response), status=500, mimetype='application/json'
    )

  refused = [
      result['entity_type']
      for result in summary['results']
      if not result['committed']
  ]

  response['status'] = 'Failed' if refused else 'Success'
  response['summary'] = summary
  if refused:
    response['message'] = (
        f'Detections refused for: {", ".join(refused)}. Previous detections '
        'are unchanged.'
    )
    logger.error('%s', response['message'])
    return flask.Response(
        flask.json.dumps(response), status=500, mimetype='application/json'
    )

  response['message'] = 'Keyword detection finished.'
  logger.info('Keyword detection finished.')
  return flask.Response(
      flask.json.dumps(response), status=200, mimetype='application/json'
  )
