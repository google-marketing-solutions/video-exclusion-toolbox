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

"""Pure BigQuery SQL generators for the Gemini video and thumbnail evaluator."""

import dataclasses
import re

from gemini_video_evaluator.utils import config as config_lib

SOURCE = 'first_party'
DETECTOR = 'gemini_thumbnail_metadata_eval'
ENTITY_VIDEO = 'video'

MODALITY_THUMBNAIL = 'thumbnail'
MODALITY_THUMBNAIL_AND_METADATA = 'thumbnail_and_metadata'

TAXONOMY_AUDIT_EVALUATED = 'audit.evaluated'
TAXONOMY_AGE_MINOR_PRESENCE = 'age_eval.minor_presence'
TAXONOMY_CONTENT_PREFIX = 'content_eval.'

LABEL_EVALUATED = 'evaluated'
LABEL_MINOR_PRESENCE = 'minor_presence'

# Every detection_policy row whose id starts with this belongs to the Config
# Sheet: the sync creates and updates these rows to match the Sheet, switches
# off (never deletes) any the Sheet no longer lists, and never touches a row
# with any other id.
POLICY_ID_PREFIX = 'first_party_gemini_'
POLICY_ID_MINOR_PRESENCE = f'{POLICY_ID_PREFIX}minor_presence'
POLICY_ID_CONTENT_PREFIX = f'{POLICY_ID_PREFIX}content_'
# Below the 100 of the seeded keyword policy, so that a policy added by hand
# at the usual priority overrides the ones generated from the Sheet.
POLICY_PRIORITY = 50
# The minor_presence score is either 1.0 or 0.0.
MINOR_PRESENCE_MIN_SCORE = 1.0
# How many consecutive batch row failures are allowed before a video counts as
# evaluated and waits for the normal re-evaluation window.
MAX_FAILED_ATTEMPTS = 3

_IDENTIFIER = re.compile(r'^[A-Za-z0-9_-]+$')


def _check_identifier(value: str, what: str) -> str:
  """Rejects anything that cannot be a bare BigQuery identifier."""
  if not value or not _IDENTIFIER.match(value):
    raise ValueError(f'Invalid {what}: {value!r}')
  return value


def qualified_table(project: str, dataset: str, table: str) -> str:
  """Returns a fully qualified, backquoted BigQuery table reference."""
  _check_identifier(project, 'project')
  _check_identifier(dataset, 'dataset')
  _check_identifier(table, 'table')
  return f'`{project}.{dataset}.{table}`'


@dataclasses.dataclass(frozen=True)
class PolicyRow:
  """One detection_policy row generated from the Config Sheet."""

  policy_id: str
  taxonomy_prefix: str
  min_score: float
  action: str
  enabled: bool
  notes: str


def _policy_action(mode: str) -> str:
  """Maps a Sheet mode to a policy action ('shadow' or 'exclude')."""
  # A Disabled row keeps an action because the column is required, but it is
  # switched off through `enabled`, as the table asks for instead of deletion.
  return (
      config_lib.MODE_SHADOW.lower()
      if mode == config_lib.MODE_DISABLED
      else mode.lower()
  )


def _policy_notes(tab: str) -> str:
  return (
      f'Managed by vet-gemini-video-evaluator from the Config Sheet ({tab}'
      ' tab). Manual edits are overwritten on the next run.'
  )


def build_policy_rows(sheet_config: config_lib.SheetConfig) -> list[PolicyRow]:
  """Translates the Config Sheet into detection_policy rows."""
  # Always includes one row for the age check and one per category in the Sheet
  # (including Disabled ones) so every rule in the Sheet is visible in the
  # table.
  rows = [
      PolicyRow(
          policy_id=POLICY_ID_MINOR_PRESENCE,
          taxonomy_prefix=TAXONOMY_AGE_MINOR_PRESENCE,
          min_score=MINOR_PRESENCE_MIN_SCORE,
          action=_policy_action(sheet_config.age_check_mode),
          enabled=sheet_config.age_check_active,
          notes=_policy_notes(config_lib.SETTINGS_TAB),
      )
  ]
  for cat in sheet_config.categories:
    rows.append(
        PolicyRow(
            policy_id=f'{POLICY_ID_CONTENT_PREFIX}{cat.name}',
            taxonomy_prefix=f'{TAXONOMY_CONTENT_PREFIX}{cat.name}',
            min_score=cat.min_score,
            action=_policy_action(cat.mode),
            enabled=cat.active,
            notes=_policy_notes(config_lib.CATEGORIES_TAB),
        )
    )
  return rows


def build_read_settings_sql(
    project: str, dataset: str, settings_table: str = 'gemini_settings'
) -> str:
  """Builds the SELECT over the gemini_settings external table."""
  return (
      'SELECT setting, value FROM'
      f' {qualified_table(project, dataset, settings_table)}'
  )


def build_read_categories_sql(
    project: str, dataset: str, categories_table: str = 'gemini_categories'
) -> str:
  """Builds the SELECT over the gemini_categories external table."""
  table = qualified_table(project, dataset, categories_table)
  return f'SELECT category, mode, min_score, description FROM {table}'


def build_sync_detection_policy_sql(
    project: str, dataset: str, policy_table: str = 'detection_policy'
) -> str:
  """Builds the MERGE that makes detection_policy match the Config Sheet."""
  # A Sheet-managed row no longer in `@policies` is switched off rather than
  # deleted (per the detection_policy schema), and `AND T.enabled` makes that a
  # one-time write so the removal date in `notes` is preserved. Rows with any
  # other prefix (such as the keyword policy or manual rules) are untouched.
  target = qualified_table(project, dataset, policy_table)
  return f"""MERGE {target} AS T
USING (
  SELECT policy_id, taxonomy_prefix, min_score, action, enabled, notes
  FROM UNNEST(@policies)
) AS S
ON T.policy_id = S.policy_id
WHEN MATCHED THEN UPDATE SET
  entity_type = '{ENTITY_VIDEO}',
  source = '{SOURCE}',
  detector = '{DETECTOR}',
  taxonomy_prefix = S.taxonomy_prefix,
  min_score = S.min_score,
  action = S.action,
  priority = {POLICY_PRIORITY},
  enabled = S.enabled,
  notes = S.notes
WHEN NOT MATCHED BY TARGET THEN INSERT (
  policy_id, entity_type, source, detector, taxonomy_prefix,
  min_score, action, priority, enabled, notes
) VALUES (
  S.policy_id, '{ENTITY_VIDEO}', '{SOURCE}', '{DETECTOR}', S.taxonomy_prefix,
  S.min_score, S.action, {POLICY_PRIORITY}, S.enabled, S.notes
)
WHEN NOT MATCHED BY SOURCE
  AND STARTS_WITH(T.policy_id, '{POLICY_ID_PREFIX}')
  AND T.enabled THEN UPDATE SET
  enabled = FALSE,
  notes = CONCAT(
    'Switched off by vet-gemini-video-evaluator on ',
    FORMAT_DATE('%F', CURRENT_DATE()),
    ': the Config Sheet no longer lists this check.'
  )
"""


def build_recompute_age_scores_sql(
    project: str,
    dataset: str,
    detection_table: str = 'detection',
) -> str:
  """Builds the UPDATE that recomputes minor_presence scores in place."""
  # Uses the preserved `$.min_real_human_age` in `evidence` on active rows whose
  # recorded `$.max_minor_age_threshold` differs from `@max_minor_age`, updating
  # `score` to 1.0 or 0.0 without calling Gemini again.
  target = qualified_table(project, dataset, detection_table)
  return f"""UPDATE {target}
SET
  score = IF(
    SAFE_CAST(JSON_VALUE(evidence, '$.min_real_human_age') AS INT64) <= @max_minor_age,
    1.0,
    0.0
  ),
  evidence = JSON_SET(evidence, '$.max_minor_age_threshold', @max_minor_age)
WHERE entity_type = '{ENTITY_VIDEO}'
  AND source = '{SOURCE}'
  AND detector = '{DETECTOR}'
  AND taxonomy = '{TAXONOMY_AGE_MINOR_PRESENCE}'
  AND retracted_at IS NULL
  AND IFNULL(
    SAFE_CAST(JSON_VALUE(evidence, '$.max_minor_age_threshold') AS INT64),
    -1
  ) != @max_minor_age
"""


def build_candidate_discovery_sql(
    project: str,
    dataset: str,
    corpus_table: str = 'youtube_video',
    detection_table: str = 'detection',
    videos_to_exclude_view: str = 'VideosToExclude',
) -> str:
  """Builds the SELECT that picks the next videos to send to Gemini."""
  # A video is picked if either:
  #   1. it has never been evaluated and is not already in VideosToExclude for
  #      another reason (such as a keyword match); or
  #   2. its last evaluation is older than `@reevaluate_before` (the exclusion
  #      check is skipped here so a video excluded by its own earlier Gemini
  #      scores can be re-evaluated and leave the list).
  # Never-evaluated videos come first, then re-evaluations, ordered by most
  # recently updated first.
  corpus = qualified_table(project, dataset, corpus_table)
  detection = qualified_table(project, dataset, detection_table)
  to_exclude = qualified_table(project, dataset, videos_to_exclude_view)

  return f"""WITH corpus AS (
  SELECT
    video_id,
    title,
    description,
    ARRAY_TO_STRING(tags, ', ') AS tags,
    availability_status,
    datetime_updated
  FROM {corpus}
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY video_id ORDER BY datetime_updated DESC) = 1
),
evaluated AS (
  SELECT
    entity_id AS video_id,
    MAX(last_detected_at) AS last_evaluated_at
  FROM {detection}
  WHERE entity_type = '{ENTITY_VIDEO}'
    AND detector = '{DETECTOR}'
    AND taxonomy = '{TAXONOMY_AUDIT_EVALUATED}'
    AND retracted_at IS NULL
    AND (
      IFNULL(JSON_VALUE(evidence, '$.status'), 'ok') = 'ok'
      OR IFNULL(
        SAFE_CAST(JSON_VALUE(evidence, '$.failed_attempts') AS INT64),
        1
      ) >= {MAX_FAILED_ATTEMPTS}
    )
  GROUP BY entity_id
)
SELECT
  c.video_id,
  IFNULL(c.title, '') AS title,
  IFNULL(c.description, '') AS description,
  IFNULL(c.tags, '') AS tags,
  e.video_id IS NOT NULL AS reevaluation
FROM corpus AS c
LEFT JOIN evaluated AS e ON e.video_id = c.video_id
WHERE (c.availability_status IS NULL OR c.availability_status != 'DELETED')
  AND (
    (
      e.video_id IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM {to_exclude} AS x
        WHERE x.video_id = c.video_id
      )
    )
    OR e.last_evaluated_at < @reevaluate_before
  )
ORDER BY reevaluation, c.datetime_updated DESC, c.video_id
LIMIT @max_videos
"""


def build_merge_detections_sql(
    project: str,
    dataset: str,
    staging_table: str,
    detection_table: str = 'detection',
) -> str:
  """Builds the idempotent MERGE of a batch staging table into `detection`."""
  # Updates existing rows for the evaluated entities/labels and inserts new
  # rows without retracting detections for videos outside the current batch.
  target = qualified_table(project, dataset, detection_table)
  source = qualified_table(project, dataset, staging_table)

  return f"""MERGE {target} AS target
USING (
  SELECT
    entity_type, entity_id, modality, source, detector,
    detector_version, taxonomy, label, score,
    PARSE_JSON(evidence) AS evidence
  FROM {source}
) AS source
ON  target.entity_type = source.entity_type
  AND target.source = source.source
  AND target.detector = source.detector
  AND target.entity_id = source.entity_id
  AND target.modality = source.modality
  AND target.label = source.label
WHEN MATCHED THEN UPDATE SET
  detector_version = source.detector_version,
  taxonomy = source.taxonomy,
  score = source.score,
  evidence = IF(
    JSON_VALUE(source.evidence, '$.status') = 'failed'
      AND JSON_VALUE(target.evidence, '$.status') = 'failed',
    JSON_SET(
      source.evidence,
      '$.failed_attempts',
      IFNULL(
        SAFE_CAST(JSON_VALUE(target.evidence, '$.failed_attempts') AS INT64),
        1
      ) + 1
    ),
    source.evidence
  ),
  last_detected_at = @run_ts,
  retracted_at = NULL
WHEN NOT MATCHED BY TARGET THEN INSERT (
  entity_type, entity_id, modality, source, detector, detector_version,
  taxonomy, label, score, evidence, first_detected_at, last_detected_at
) VALUES (
  source.entity_type, source.entity_id, source.modality, source.source,
  source.detector, source.detector_version, source.taxonomy, source.label,
  source.score, source.evidence, @run_ts, @run_ts
)
"""
