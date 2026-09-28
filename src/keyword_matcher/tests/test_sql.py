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

"""Unit tests for the generated BigQuery statements."""

from keyword_matcher.utils import sql
import pytest

_PROJECT = 'test-project'
_DATASET = 'video_exclusion_toolbox'
_STAGING = 'keyword_match_staging_video'


def _video_staging() -> str:
  return sql.build_staging_sql(_PROJECT, _DATASET, _STAGING, sql.ENTITY_VIDEO)


def _channel_staging() -> str:
  return sql.build_staging_sql(_PROJECT, _DATASET, _STAGING, sql.ENTITY_CHANNEL)


def _all_statements() -> list[str]:
  return [
      _video_staging(),
      _channel_staging(),
      sql.build_merge_sql(_PROJECT, _DATASET, _STAGING, sql.ENTITY_VIDEO),
      sql.build_merge_sql(_PROJECT, _DATASET, _STAGING, sql.ENTITY_CHANNEL),
      sql.build_previous_counts_sql(_PROJECT, _DATASET, sql.ENTITY_VIDEO),
      sql.build_staging_counts_sql(_PROJECT, _DATASET, _STAGING),
  ]


def test_no_statement_uses_regexp_extract_all():
  """Attribution by extraction would silently drop adjacent keywords.

  The boundary classes consume their delimiter, unlike the zero-width \\b they
  replace, so RE2 cannot return two keywords separated by a single space.
  Existence testing is immune. This test exists because the fault is invisible
  in casual testing -- non-adjacent keywords behave correctly -- and a future
  optimisation pass would otherwise be free to reintroduce it.
  """
  for statement in _all_statements():
    assert 'REGEXP_EXTRACT_ALL' not in statement.upper()


def test_keywords_are_passed_as_parameters_not_interpolated():
  statement = _video_staging()
  assert '@keyword_patterns' in statement
  assert '@admission_pattern' in statement


def test_video_statement_reads_the_video_corpus():
  statement = _video_staging()
  assert f'`{_PROJECT}.{_DATASET}.youtube_video`' in statement
  assert 'video_id AS entity_id' in statement
  assert "ARRAY_TO_STRING(tags, ', ') AS tags_flat" in statement


def test_video_statement_covers_three_modalities():
  statement = _video_staging()
  for modality in ('title', 'description', 'tags'):
    assert f"STRUCT('{modality}' AS modality" in statement


def test_channel_statement_reads_the_channel_corpus():
  statement = _channel_staging()
  assert f'`{_PROJECT}.{_DATASET}.youtube_channel`' in statement
  assert 'channel_id AS entity_id' in statement


def test_channel_statement_has_no_tags_modality():
  """Channels have no tags column."""
  statement = _channel_staging()
  assert "STRUCT('tags' AS modality" not in statement
  assert 'tags_flat' not in statement


def test_channel_statement_matches_on_description():
  """Channel matching covers both title and description modalities."""
  statement = _channel_staging()
  assert "STRUCT('description' AS modality" in statement


def test_statements_deduplicate_the_corpus():
  assert 'QUALIFY ROW_NUMBER() OVER (' in _video_staging()
  assert 'PARTITION BY video_id ORDER BY datetime_updated DESC' in (
      _video_staging()
  )
  assert 'PARTITION BY channel_id ORDER BY datetime_updated DESC' in (
      _channel_staging()
  )


def test_admission_concatenates_fields_with_a_separator():
  """Without a separator a keyword could match across a field boundary."""
  statement = _video_staging()
  assert (
      "CONCAT(IFNULL(title, ''), ' ', IFNULL(description, ''), ' ',"
      " IFNULL(tags_flat, ''))"
      in statement
  )


def test_admission_is_case_insensitive():
  assert 'LOWER(CONCAT(' in _video_staging()


def test_attribution_is_guarded_by_a_literal_prefilter():
  """The prefilter is a performance requirement, not an optimisation.

  keywords.pattern is a column, so the regular expression is recompiled on
  every evaluation -- once per admitted entity, per keyword, per modality.
  Without the literal substring guard, per-row regex recompilation across
  multi-million-row corpora causes multi-minute query runtimes. It is
  semantically inert: the boundary pattern can only match where the keyword
  appears literally.

  IF matters too. BigQuery guarantees that a conditional expression evaluates
  only the branch taken; it makes no such guarantee about the order of AND
  operands, so writing this as a conjunction would not reliably skip the
  regex.
  """
  statement = _video_staging()
  assert 'STRPOS(modality.text, keywords.keyword) > 0' in statement
  assert 'WHERE IF(' in statement
  assert 'REGEXP_CONTAINS(modality.text, keywords.pattern)' in statement


def test_merge_retracts_only_this_detectors_active_rows():
  """The scoping on NOT MATCHED BY SOURCE is load-bearing.

  Without it, this detector's merge would retract the rows written by the
  Gemini detectors as soon as those exist. Guarding on target.retracted_at IS
  NULL preserves the original retraction timestamp across subsequent runs.
  """
  statement = sql.build_merge_sql(
      _PROJECT, _DATASET, _STAGING, sql.ENTITY_VIDEO
  )
  _, retract_clause = statement.split('WHEN NOT MATCHED BY SOURCE')
  assert "target.entity_type = 'video'" in retract_clause
  assert "target.source = 'first_party'" in retract_clause
  assert "target.detector = 'keyword_match'" in retract_clause
  assert 'target.retracted_at IS NULL' in retract_clause
  assert 'THEN UPDATE SET' in retract_clause
  assert 'retracted_at = @run_ts' in retract_clause


def test_merge_preserves_first_detected_at():
  statement = sql.build_merge_sql(
      _PROJECT, _DATASET, _STAGING, sql.ENTITY_VIDEO
  )
  matched_clause = statement.split('WHEN MATCHED THEN UPDATE SET')[1].split(
      'WHEN NOT MATCHED BY TARGET'
  )[0]
  assert 'first_detected_at' not in matched_clause
  assert 'last_detected_at = @run_ts' in matched_clause


def test_merge_writes_the_constant_detector_identity():
  statement = sql.build_merge_sql(
      _PROJECT, _DATASET, _STAGING, sql.ENTITY_CHANNEL
  )
  assert "'channel', source.entity_id" in statement
  assert f"'{sql.TAXONOMY}'" in statement
  assert '@detector_version' in statement


def test_previous_counts_are_scoped_to_this_detector():
  statement = sql.build_previous_counts_sql(
      _PROJECT, _DATASET, sql.ENTITY_VIDEO
  )
  assert "entity_type = 'video'" in statement
  assert "detector = 'keyword_match'" in statement
  assert 'GROUP BY label' in statement


def test_unknown_entity_type_is_rejected():
  with pytest.raises(ValueError):
    sql.build_staging_sql(_PROJECT, _DATASET, _STAGING, 'playlist')
  with pytest.raises(ValueError):
    sql.build_merge_sql(_PROJECT, _DATASET, _STAGING, 'playlist')


def test_identifiers_are_validated():
  with pytest.raises(ValueError):
    sql.qualified_table('project`; DROP TABLE x; --', _DATASET, _STAGING)
  with pytest.raises(ValueError):
    sql.qualified_table(_PROJECT, 'dataset with spaces', _STAGING)


def test_staging_tables_expire():
  """An abandoned run must not leave staging tables behind indefinitely."""
  assert 'expiration_timestamp' in _video_staging()


def test_staging_sql_accepts_source_table_override():
  statement = sql.build_staging_sql(
      _PROJECT,
      _DATASET,
      _STAGING,
      sql.ENTITY_VIDEO,
      source_table='custom_video_corpus',
  )
  assert f'`{_PROJECT}.{_DATASET}.custom_video_corpus`' in statement
  assert f'`{_PROJECT}.{_DATASET}.youtube_video`' not in statement
