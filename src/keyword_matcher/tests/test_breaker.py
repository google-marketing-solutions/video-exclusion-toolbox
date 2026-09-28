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

"""Unit tests for the detection circuit breaker.

The fixtures model a skewed distribution across 100 keywords in which detection
volume is concentrated in a small head of high-frequency keywords.
"""

from keyword_matcher.utils import breaker

# Synthetic head distribution where the top keyword alone accounts for >10% of
# total detection volume, exercising the shared-set retention logic.
_TOP_KEYWORDS = {
    'kw_head_1': 15000,
    'kw_head_2': 10000,
    'kw_head_3': 8000,
    'kw_head_4': 6000,
    'kw_head_5': 5000,
}

_FILLER_KEYWORD_COUNT = 95
_FILLER_ROWS = 100


def _skewed_previous_counts() -> dict[str, int]:
  """Builds a previous run with a skewed keyword distribution."""
  counts = dict(_TOP_KEYWORDS)
  for index in range(_FILLER_KEYWORD_COUNT):
    counts[f'filler{index:03d}'] = _FILLER_ROWS
  return counts


def test_empty_keyword_list_aborts():
  decision = breaker.evaluate(
      current_keywords=[],
      previous_counts={'alpha': 10},
      staging_counts={},
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_NO_KEYWORDS


def test_keywords_matching_nothing_aborts():
  decision = breaker.evaluate(
      current_keywords=['alpha', 'beta'],
      previous_counts={'alpha': 10},
      staging_counts={},
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_NO_DETECTIONS


def test_first_run_seeds_without_comparison():
  decision = breaker.evaluate(
      current_keywords=['alpha', 'beta'],
      previous_counts={},
      staging_counts={'alpha': 5, 'beta': 3},
  )
  assert decision.proceed
  assert decision.seeding_run
  assert decision.metrics.keyword_retention_ratio is None


def test_deleting_one_high_volume_keyword_does_not_abort():
  """The false abort that a plain volume ratio at 0.9 would have produced."""
  previous = _skewed_previous_counts()
  current_keywords = [key for key in previous if key != 'kw_head_1']
  staging = {key: previous[key] for key in current_keywords}

  # A plain ratio over all detections would fail at this threshold: removing
  # the single largest keyword costs >10% of the volume.
  plain_ratio = sum(staging.values()) / sum(previous.values())
  assert plain_ratio < 0.9

  decision = breaker.evaluate(
      current_keywords=current_keywords,
      previous_counts=previous,
      staging_counts=staging,
  )
  assert decision.proceed
  assert decision.metrics.yield_retention_ratio == 1.0


def test_truncated_read_aborts_on_the_keyword_set_assertion():
  """The blind spot: the yield assertion alone would pass this run.

  A Drive read returning 8 of 100 keywords restricts both sides of the yield
  comparison to those 8, so the yield ratio is 1.0. Only the keyword-set
  assertion catches it.
  """
  previous = _skewed_previous_counts()
  surviving = sorted(previous)[:8]
  staging = {key: previous[key] for key in surviving}

  decision = breaker.evaluate(
      current_keywords=surviving,
      previous_counts=previous,
      staging_counts=staging,
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_KEYWORD_SET_SHRANK
  # The assertion that would have let this through, recorded for the record.
  assert decision.metrics.yield_retention_ratio == 1.0
  assert decision.metrics.keyword_retention_ratio < 0.1


def test_acknowledgement_waives_the_keyword_set_assertion():
  previous = _skewed_previous_counts()
  surviving = sorted(previous)[:8]
  staging = {key: previous[key] for key in surviving}

  decision = breaker.evaluate(
      current_keywords=surviving,
      previous_counts=previous,
      staging_counts=staging,
      ack_keyword_set_change=True,
  )
  assert decision.proceed
  assert decision.warnings


def test_acknowledgement_does_not_waive_the_yield_assertion():
  """A bulk keyword edit is no excuse for the survivors collapsing."""
  previous = _skewed_previous_counts()
  surviving = sorted(previous)[:8]
  staging = {key: previous[key] // 4 for key in surviving}

  decision = breaker.evaluate(
      current_keywords=surviving,
      previous_counts=previous,
      staging_counts=staging,
      ack_keyword_set_change=True,
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_YIELD_DROPPED


def test_wholly_corrupted_read_aborts():
  """Every keyword mangled, so nothing overlaps and the ratio is undefined."""
  previous = _skewed_previous_counts()
  mangled = [f'{key}\ufffd' for key in previous]

  decision = breaker.evaluate(
      current_keywords=mangled,
      previous_counts=previous,
      staging_counts={key: 10 for key in mangled},
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_KEYWORD_SET_COLLAPSED


def test_wholly_corrupted_read_is_not_waivable():
  previous = _skewed_previous_counts()
  mangled = [f'{key}\ufffd' for key in previous]

  decision = breaker.evaluate(
      current_keywords=mangled,
      previous_counts=previous,
      staging_counts={key: 10 for key in mangled},
      ack_keyword_set_change=True,
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_KEYWORD_SET_COLLAPSED


def test_yield_drop_below_threshold_aborts():
  previous = _skewed_previous_counts()
  keywords = list(previous)
  staging = {key: int(value * 0.85) for key, value in previous.items()}

  decision = breaker.evaluate(
      current_keywords=keywords,
      previous_counts=previous,
      staging_counts=staging,
  )
  assert not decision.proceed
  assert decision.reason == breaker.ABORT_YIELD_DROPPED
  assert decision.metrics.yield_retention_ratio < 0.9


def test_large_increase_passes():
  """A large increase in detections passes; the assertions only guard against drops."""
  previous = _skewed_previous_counts()
  keywords = list(previous)
  staging = {key: value * 12 for key, value in previous.items()}

  decision = breaker.evaluate(
      current_keywords=keywords,
      previous_counts=previous,
      staging_counts=staging,
  )
  assert decision.proceed
  assert decision.metrics.yield_retention_ratio == 12.0


def test_single_keyword_regression_warns_without_blocking():
  previous = _skewed_previous_counts()
  keywords = list(previous)
  staging = dict(previous)
  staging['filler000'] = int(_FILLER_ROWS * 0.4)

  decision = breaker.evaluate(
      current_keywords=keywords,
      previous_counts=previous,
      staging_counts=staging,
  )
  assert decision.proceed
  assert any('filler000' in warning for warning in decision.warnings)


def test_keywords_with_no_matches_still_count_towards_the_keyword_set():
  """The keyword set is the validated list, not the labels that matched.

  Otherwise a keyword that legitimately finds nothing would look like a
  keyword that vanished from the sheet.
  """
  previous = {'alpha': 100, 'beta': 100}
  decision = breaker.evaluate(
      current_keywords=['alpha', 'beta', 'gamma'],
      previous_counts=previous,
      staging_counts={'alpha': 100, 'beta': 100},
  )
  assert decision.proceed
  assert decision.metrics.keyword_count == 3
  assert decision.metrics.shared_keyword_count == 2
