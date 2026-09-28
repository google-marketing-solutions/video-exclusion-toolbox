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

"""Generates the BigQuery statements the keyword detector executes.

Python owns the matching logic; BigQuery moves the data. Every function here is
pure and returns a string, so the generated SQL can be asserted in unit tests
without a BigQuery client.

Keywords and patterns are never interpolated into the SQL text. They are passed
as query parameters, which keeps the statement identical between runs and keeps
sheet content out of the query string entirely.
"""

import re
from typing import NamedTuple, Optional

# Constant detector identity. Every row this detector writes carries these.
SOURCE = 'first_party'
DETECTOR = 'keyword_match'

# The keyword sheet carries no category information, so a single constant
# taxonomy is used rather than inventing categories that do not exist. If the
# sheet later gains a category column, this becomes meaningful with no change
# required of consumers, because consumers key on taxonomy rather than on
# detector identity.
TAXONOMY = 'advertiser_blocklist.keyword'

ENTITY_VIDEO = 'video'
ENTITY_CHANNEL = 'channel'

# Staging tables are disposable. An expiry means an abandoned run -- a crash
# between creation and cleanup -- cannot leave tables behind indefinitely.
_STAGING_EXPIRY_HOURS = 6

_IDENTIFIER = re.compile(r'^[A-Za-z0-9_-]+$')


class EntityConfig(NamedTuple):
  """How one entity type is read out of the corpus.

  Attributes:
      entity_type: The value written to detection.entity_type.
      source_table: Unqualified corpus table name.
      id_column: The column holding the entity identifier.
      modalities: Which text fields are examined, in output order.
  """

  entity_type: str
  source_table: str
  id_column: str
  modalities: tuple[str, ...]


ENTITY_CONFIGS = {
    ENTITY_VIDEO: EntityConfig(
        entity_type=ENTITY_VIDEO,
        source_table='youtube_video',
        id_column='video_id',
        modalities=('title', 'description', 'tags'),
    ),
    # Channels have no tags column. They carry title and description; a large
    # share of channel candidates match on description alone, so examining both
    # text fields is required.
    ENTITY_CHANNEL: EntityConfig(
        entity_type=ENTITY_CHANNEL,
        source_table='youtube_channel',
        id_column='channel_id',
        modalities=('title', 'description'),
    ),
}

# The column inside the corpus CTE that carries each modality's text.
_MODALITY_COLUMNS = {
    'title': 'title',
    'description': 'description',
    'tags': 'tags_flat',
}


def _check_identifier(value: str, what: str) -> str:
  """Rejects anything that cannot be a bare BigQuery identifier.

  These values come from deployment configuration rather than user input, but
  they are interpolated into SQL text, so they are checked rather than trusted.

  Args:
      value: The candidate identifier.
      what: Name of the argument, used in the error message.

  Returns:
      The value, unchanged.

  Raises:
      ValueError: If the value is not a bare identifier.
  """
  if not _IDENTIFIER.match(value):
    raise ValueError(f'Invalid {what}: {value!r}')
  return value


def qualified_table(project: str, dataset: str, table: str) -> str:
  """Returns a fully qualified, backquoted table reference."""
  _check_identifier(project, 'project')
  _check_identifier(dataset, 'dataset')
  _check_identifier(table, 'table')
  return f'`{project}.{dataset}.{table}`'


def build_staging_sql(
    project: str,
    dataset: str,
    staging_table: str,
    entity_type: str,
    source_table: Optional[str] = None,
) -> str:
  """Builds the statement that computes this run's detections.

  The shape is two-stage and that is deliberate. An admission filter reduces
  the corpus to entities containing any keyword at all, in a single pass; only
  that subset is then cross-joined against the keyword list for attribution.
  Cross-joining the whole corpus would multiply millions of rows by the keyword
  count.

  Args:
      project: Google Cloud project holding the dataset.
      dataset: BigQuery dataset holding both corpus and detection tables.
      staging_table: Name of the table to create.
      entity_type: Either 'video' or 'channel'.
      source_table: Optional unqualified corpus table name override. Defaults to
        the entity type's canonical table in ENTITY_CONFIGS.

  Returns:
      A CREATE OR REPLACE TABLE statement.

  Raises:
      ValueError: If entity_type is unknown.
  """
  if entity_type not in ENTITY_CONFIGS:
    raise ValueError(f'Unknown entity type: {entity_type!r}')
  config = ENTITY_CONFIGS[entity_type]

  destination = qualified_table(project, dataset, staging_table)
  source = qualified_table(
      project, dataset, source_table or config.source_table
  )

  corpus_columns = [f'{config.id_column} AS entity_id', 'title', 'description']
  if 'tags' in config.modalities:
    corpus_columns.append("ARRAY_TO_STRING(tags, ', ') AS tags_flat")

  # A space literal is concatenated between fields. Without it a title ending
  # "sp" followed by a description beginning "ort" would admit on "sport",
  # producing an entity the attribution stage then finds nothing to attribute.
  admission_parts = ", ' ', ".join(
      f"IFNULL({_MODALITY_COLUMNS[modality]}, '')"
      for modality in config.modalities
  )

  modality_structs = ',\n  '.join(
      "STRUCT('{modality}' AS modality, "
      "LOWER(IFNULL(corpus.{column}, '')) AS text)".format(
          modality=modality,
          column=_MODALITY_COLUMNS[modality],
      )
      for modality in config.modalities
  )

  return f"""CREATE OR REPLACE TABLE {destination}
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(
      CURRENT_TIMESTAMP(), INTERVAL {_STAGING_EXPIRY_HOURS} HOUR)
) AS
WITH
keywords AS (
  SELECT keyword, pattern FROM UNNEST(@keyword_patterns)
),
corpus AS (
  SELECT
    {', '.join(corpus_columns)}
  FROM {source}
  -- The append-only ingestion pipeline can write duplicate snapshots per
  -- identifier over time. Deduplicating to the latest row per identifier
  -- prevents emitting duplicate detections for the same entity and keyword.
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY {config.id_column} ORDER BY datetime_updated DESC) = 1
),
admitted AS (
  SELECT * FROM corpus
  WHERE REGEXP_CONTAINS(
      LOWER(CONCAT({admission_parts})),
      @admission_pattern)
)
SELECT
  corpus.entity_id,
  modality.modality,
  keywords.keyword AS label
FROM admitted AS corpus
CROSS JOIN keywords
CROSS JOIN UNNEST([
  {modality_structs}
]) AS modality
-- Attribution is a per-keyword existence test, never an extraction over the
-- combined pattern. The boundary classes consume their delimiter, so an
-- extraction would silently drop the second of two adjacent keywords.
--
-- The STRPOS guard is a necessary-condition prefilter, not a second opinion.
-- The boundary pattern can only match where the keyword occurs literally --
-- the optional plural suffix still leaves the keyword itself present -- so
-- admitting only rows containing the literal cannot change the result.
--
-- It is load-bearing for performance. keywords.pattern is a column rather
-- than a constant, so the engine recompiles the expression on every
-- evaluation: once per admitted entity, per keyword, per modality. Without
-- this literal substring prefilter, per-row regex recompilation across
-- multi-million-row tables causes multi-minute query runtimes.
--
-- IF is used rather than AND because BigQuery guarantees that conditional
-- expressions evaluate only the branch taken, whereas the order in which
-- conjuncts are evaluated is not guaranteed.
WHERE IF(
    STRPOS(modality.text, keywords.keyword) > 0,
    REGEXP_CONTAINS(modality.text, keywords.pattern),
    FALSE)
"""


def build_merge_sql(
    project: str,
    dataset: str,
    staging_table: str,
    entity_type: str,
    detection_table: str = 'detection',
) -> str:
  """Builds the statement that applies this run's detections.

  Merging rather than replacing means first_detected_at survives across runs,
  so match history exists even though every run recomputes from scratch.

  Args:
      project: Google Cloud project holding the dataset.
      dataset: BigQuery dataset holding the detection table.
      staging_table: The table produced by build_staging_sql.
      entity_type: Either 'video' or 'channel'.
      detection_table: Name of the detection table.

  Returns:
      A MERGE statement.

  Raises:
      ValueError: If entity_type is unknown.
  """
  if entity_type not in ENTITY_CONFIGS:
    raise ValueError(f'Unknown entity type: {entity_type!r}')

  target = qualified_table(project, dataset, detection_table)
  source = qualified_table(project, dataset, staging_table)

  # Every clause is scoped to this detector, this source and this entity type.
  # Without that scoping the NOT MATCHED BY SOURCE branch would delete the
  # rows written by the Gemini detectors once they exist.
  scope = (
      f"target.entity_type = '{entity_type}'\n"
      f"  AND target.source = '{SOURCE}'\n"
      f"  AND target.detector = '{DETECTOR}'"
  )

  return f"""MERGE {target} AS target
USING (
  SELECT entity_id, modality, label FROM {source}
) AS source
ON  {scope}
  AND target.entity_id = source.entity_id
  AND target.modality = source.modality
  AND target.label = source.label
WHEN MATCHED THEN UPDATE SET
  last_detected_at = @run_ts,
  detector_version = @detector_version,
  retracted_at = NULL
WHEN NOT MATCHED BY TARGET THEN INSERT (
  entity_type, entity_id, modality, source, detector, detector_version,
  taxonomy, label, score, evidence, first_detected_at, last_detected_at
) VALUES (
  '{entity_type}', source.entity_id, source.modality, '{SOURCE}',
  '{DETECTOR}', @detector_version, '{TAXONOMY}', source.label,
  NULL, NULL, @run_ts, @run_ts
)
-- Removing a keyword from the sheet retracts its detections on the next run.
WHEN NOT MATCHED BY SOURCE AND {scope} AND target.retracted_at IS NULL
THEN UPDATE SET
  retracted_at = @run_ts
"""


def build_previous_counts_sql(
    project: str,
    dataset: str,
    entity_type: str,
    detection_table: str = 'detection',
) -> str:
  """Builds the query returning the previous run's detections per keyword.

  Args:
      project: Google Cloud project holding the dataset.
      dataset: BigQuery dataset holding the detection table.
      entity_type: Either 'video' or 'channel'.
      detection_table: Name of the detection table.

  Returns:
      A SELECT statement returning label and detections.

  Raises:
      ValueError: If entity_type is unknown.
  """
  if entity_type not in ENTITY_CONFIGS:
    raise ValueError(f'Unknown entity type: {entity_type!r}')

  target = qualified_table(project, dataset, detection_table)
  return f"""SELECT label, COUNT(*) AS detections
FROM {target}
WHERE entity_type = '{entity_type}'
  AND source = '{SOURCE}'
  AND detector = '{DETECTOR}'
  AND retracted_at IS NULL
GROUP BY label
"""


def build_staging_counts_sql(
    project: str,
    dataset: str,
    staging_table: str,
) -> str:
  """Builds the query returning this run's detections per keyword.

  Args:
      project: Google Cloud project holding the dataset.
      dataset: BigQuery dataset holding the staging table.
      staging_table: The table produced by build_staging_sql.

  Returns:
      A SELECT statement returning label and detections.
  """
  source = qualified_table(project, dataset, staging_table)
  return f"""SELECT label, COUNT(*) AS detections
FROM {source}
GROUP BY label
"""
