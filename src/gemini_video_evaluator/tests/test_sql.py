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

"""Unit tests for gemini_video_evaluator.utils.sql."""

import json
import os

from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import sql
import pytest

_SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'bq_schemas'
)
_TERRAFORM_DIR = os.path.join(_SCHEMA_DIR, '..', 'terraform')
# The taxonomy condition of the VideosToExclude and ChannelsToExclude views.
_TAXONOMY_CONDITION = (
    'AND (p.taxonomy_prefix IS NULL OR d.taxonomy = p.taxonomy_prefix OR'
    " (ENDS_WITH(p.taxonomy_prefix, '.') AND"
    ' STARTS_WITH(d.taxonomy, p.taxonomy_prefix)))'
)


def _sheet_config(age_check_mode='Exclude', categories=()):
  return config_lib.SheetConfig(
      age_check_mode=age_check_mode,
      max_minor_age=18,
      advertiser_context='',
      max_videos_per_batch=500,
      reevaluate_before=None,
      categories=tuple(categories),
  )


def _schema_columns(file_name):
  with open(os.path.join(_SCHEMA_DIR, file_name), encoding='utf-8') as f:
    return [field['name'] for field in json.load(f)]


def _policy_matches(taxonomy_prefix, taxonomy):
  """Evaluates _TAXONOMY_CONDITION in Python."""
  if taxonomy_prefix is None or taxonomy == taxonomy_prefix:
    return True
  return taxonomy_prefix.endswith('.') and taxonomy.startswith(taxonomy_prefix)


def _sync_sql():
  return sql.build_sync_detection_policy_sql(
      'test-gcp-project', 'test_dataset', 'detection_policy'
  )


def test_qualified_table_rejects_sql_injection():
  """Verifies identifier validation blocks non-identifier characters."""
  assert (
      sql.qualified_table('my-proj', 'my_dataset', 'my_table')
      == '`my-proj.my_dataset.my_table`'
  )
  with pytest.raises(ValueError, match='Invalid table'):
    sql.qualified_table('my-proj', 'my_dataset', 'my_table; DROP TABLE x')


def test_policy_rows_follow_the_sheet():
  """Verifies one row for the age check, then one per category in order."""
  rows = sql.build_policy_rows(
      _sheet_config(
          age_check_mode='Exclude',
          categories=(
              config_lib.Category('kids', 'Shadow', 0.75, 'Kids.'),
              config_lib.Category('profanity', 'Disabled', 0.7, ''),
          ),
      )
  )

  assert [row.policy_id for row in rows] == [
      'first_party_gemini_minor_presence',
      'first_party_gemini_content_kids',
      'first_party_gemini_content_profanity',
  ]
  assert [row.taxonomy_prefix for row in rows] == [
      'age_eval.minor_presence',
      'content_eval.kids',
      'content_eval.profanity',
  ]
  assert [row.min_score for row in rows] == [1.0, 0.75, 0.7]
  assert [row.action for row in rows] == ['exclude', 'shadow', 'shadow']
  assert [row.enabled for row in rows] == [True, True, False]
  assert config_lib.SETTINGS_TAB in rows[0].notes
  assert config_lib.CATEGORIES_TAB in rows[1].notes
  assert 'Manual edits are overwritten' in rows[1].notes


def test_the_age_check_always_has_a_policy_row():
  """Verifies the sync never sends an empty array, which BigQuery rejects."""
  rows = sql.build_policy_rows(_sheet_config(age_check_mode='Disabled'))

  assert len(rows) == 1
  assert not rows[0].enabled
  assert rows[0].action == 'shadow'


def test_no_policy_can_match_the_evaluated_marker():
  """Verifies the audit row never leads to an exclusion."""
  rows = sql.build_policy_rows(
      _sheet_config(
          categories=(config_lib.Category('kids', 'Exclude', 0.7, 'Kids.'),)
      )
  )

  assert not any(
      _policy_matches(row.taxonomy_prefix, sql.TAXONOMY_AUDIT_EVALUATED)
      for row in rows
  )


def test_a_category_policy_matches_only_its_own_taxonomy():
  """Verifies the kids policy does not judge kids_gaming scores."""
  rows = sql.build_policy_rows(
      _sheet_config(
          categories=(
              config_lib.Category('kids', 'Exclude', 0.7, 'Kids.'),
              config_lib.Category('kids_gaming', 'Shadow', 0.9, 'Games.'),
          )
      )
  )
  kids = rows[1]

  assert kids.policy_id == 'first_party_gemini_content_kids'
  assert _policy_matches(kids.taxonomy_prefix, 'content_eval.kids')
  assert not _policy_matches(kids.taxonomy_prefix, 'content_eval.kids_gaming')
  # A trailing dot would make a Sheet policy cover a whole subtree.
  assert not any(row.taxonomy_prefix.endswith('.') for row in rows)


def test_the_exclusion_views_match_taxonomies_exactly_or_by_subtree():
  """Verifies both views use the condition that _policy_matches evaluates.

  Sheet validation accepts kids next to kids_gaming only because of this
  condition, so a revert to a bare STARTS_WITH must fail here.
  """
  path = os.path.join(_TERRAFORM_DIR, 'resource_google_bigquery_table.tf')
  with open(path, encoding='utf-8') as f:
    terraform = f.read()

  assert terraform.count(_TAXONOMY_CONDITION) == 2
  assert 'OR STARTS_WITH(d.taxonomy, p.taxonomy_prefix))' not in terraform


def test_sync_only_touches_rows_managed_by_the_sheet():
  """Verifies the MERGE is scoped by first_party_gemini_ ids, not detector."""
  query = _sync_sql()
  _, not_matched_by_source = query.split('WHEN NOT MATCHED BY SOURCE')

  assert query.startswith(
      'MERGE `test-gcp-project.test_dataset.detection_policy` AS T'
  )
  assert 'FROM UNNEST(@policies)' in query
  assert 'ON T.policy_id = S.policy_id' in query
  assert (
      "STARTS_WITH(T.policy_id, 'first_party_gemini_')" in not_matched_by_source
  )
  # A detector scope would also switch off manual overrides.
  assert 'T.detector' not in not_matched_by_source
  assert f'priority = {sql.POLICY_PRIORITY}' in query
  assert sql.POLICY_PRIORITY == 50


def test_a_check_removed_from_the_sheet_is_switched_off_not_deleted():
  """Verifies a removed check is switched off once and never deleted."""
  query = _sync_sql()
  when_matched, _ = query.split('WHEN NOT MATCHED BY TARGET')
  _, not_matched_by_source = query.split('WHEN NOT MATCHED BY SOURCE')

  assert 'DELETE' not in query
  assert 'AND T.enabled THEN UPDATE SET' in not_matched_by_source
  assert 'enabled = FALSE' in not_matched_by_source
  assert 'notes = CONCAT(' in not_matched_by_source
  # Adding the check back switches it on again.
  assert 'enabled = S.enabled' in when_matched


def test_legacy_seeded_gemini_policies_fall_in_the_sync_scope():
  """Verifies the sync switches off the Gemini rows earlier seeds inserted.

  Earlier versions of seed_detection_policy inserted these two shadow rows at
  priorities 90 and 80. Both outrank the Sheet's rows at 50, so while enabled
  they would keep matching detections in shadow and silently cancel Exclude.
  """
  assert 'first_party_gemini_age_shadow'.startswith(sql.POLICY_ID_PREFIX)
  assert 'first_party_gemini_content_shadow'.startswith(sql.POLICY_ID_PREFIX)


def test_recompute_age_scores_sql():
  """Verifies minor_presence scores are recomputed from min_real_human_age."""
  query = sql.build_recompute_age_scores_sql(
      project='test-gcp-project',
      dataset='test_dataset',
      detection_table='detection',
  )

  assert query.startswith('UPDATE `test-gcp-project.test_dataset.detection`')
  assert (
      "SAFE_CAST(JSON_VALUE(evidence, '$.min_real_human_age') AS INT64)"
      ' <= @max_minor_age'
      in query
  )
  assert (
      "JSON_SET(evidence, '$.max_minor_age_threshold', @max_minor_age)" in query
  )
  assert (
      'IFNULL(\n'
      "    SAFE_CAST(JSON_VALUE(evidence, '$.max_minor_age_threshold') AS"
      ' INT64),\n'
      '    -1\n'
      '  ) != @max_minor_age'
      in query
  )


def test_candidate_discovery_sql():
  """Verifies which videos are picked, and in what order."""
  query = sql.build_candidate_discovery_sql(
      project='test-gcp-project',
      dataset='test_dataset',
      corpus_table='youtube_video',
      detection_table='detection',
      videos_to_exclude_view='VideosToExclude',
  )

  assert 'FROM `test-gcp-project.test_dataset.youtube_video`' in query
  assert 'QUALIFY ROW_NUMBER() OVER (' in query
  assert "c.availability_status != 'DELETED'" in query
  assert "AND detector = 'gemini_thumbnail_metadata_eval'" in query
  assert "AND taxonomy = 'audit.evaluated'" in query
  assert 'AND retracted_at IS NULL' in query
  assert "IFNULL(JSON_VALUE(evidence, '$.status'), 'ok') = 'ok'" in query
  assert f'>= {sql.MAX_FAILED_ATTEMPTS}' in query
  assert sql.MAX_FAILED_ATTEMPTS == 3
  assert 'OR e.last_evaluated_at < @reevaluate_before' in query
  assert 'ORDER BY reevaluation, c.datetime_updated DESC, c.video_id' in query
  assert query.rstrip().endswith('LIMIT @max_videos')
  # Only never-evaluated videos are checked against the exclusion list: a
  # video excluded because of its own Gemini scores must stay re-evaluable.
  never_evaluated = query.index('e.video_id IS NULL')
  exclusion_check = query.index(
      '`test-gcp-project.test_dataset.VideosToExclude`'
  )
  reevaluation = query.index('@reevaluate_before')
  assert never_evaluated < exclusion_check < reevaluation


def test_sheet_reads_select_the_columns_of_the_tab_schemas():
  """Verifies the queries match the external table schemas by name."""
  assert sql.build_read_settings_sql('p', 'd', 'gemini_settings') == (
      'SELECT setting, value FROM `p.d.gemini_settings`'
  )
  assert sql.build_read_categories_sql('p', 'd', 'gemini_categories') == (
      'SELECT category, mode, min_score, description FROM'
      ' `p.d.gemini_categories`'
  )
  assert _schema_columns('gemini_settings.json') == ['setting', 'value']
  assert _schema_columns('gemini_categories.json') == [
      'category',
      'mode',
      'min_score',
      'description',
  ]


def test_merge_detections_sql_contract():
  """Verifies the MERGE key, and that other videos are never retracted."""
  merge = sql.build_merge_detections_sql(
      project='test-gcp-project',
      dataset='test_dataset',
      staging_table='_gemini_eval_staging_20260928',
      detection_table='detection',
  )

  assert merge.startswith(
      'MERGE `test-gcp-project.test_dataset.detection` AS target'
  )
  assert (
      'FROM `test-gcp-project.test_dataset._gemini_eval_staging_20260928`'
      in merge
  )
  assert 'ON  target.entity_type = source.entity_type' in merge
  for column in ('source', 'detector', 'entity_id', 'modality', 'label'):
    assert f'AND target.{column} = source.{column}' in merge
  assert 'PARSE_JSON(evidence) AS evidence' in merge
  assert 'taxonomy = source.taxonomy' in merge
  assert "JSON_VALUE(source.evidence, '$.status') = 'failed'" in merge
  assert "JSON_VALUE(target.evidence, '$.status') = 'failed'" in merge
  assert "'$.failed_attempts'" in merge
  assert 'retracted_at = NULL' in merge
  assert 'WHEN NOT MATCHED BY TARGET THEN INSERT' in merge
  assert 'WHEN NOT MATCHED BY SOURCE' not in merge
