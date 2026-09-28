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

"""Circuit breaker that decides whether a detection run may commit.

If an emptied or unreadable keyword sheet produces zero or truncated matches,
committing those results directly would retract active detections across the
corpus. Because detections are merged into a persistent table, aborting an
anomalous run leaves the existing detections untouched and surfaces an explicit
error to the caller.

This module performs no input or output. It is handed counts and returns a
decision, so every branch can be tested without BigQuery.

Design note -- why retention is measured over the shared keyword set. A plain
volume ratio cannot distinguish "the matcher broke" from "the user deleted
keywords", and in a skewed distribution a single high-frequency keyword can
account for ~20% of detected entities, so a plain ratio cannot be tightened
beyond about 0.8 without aborting on ordinary sheet edits. Restricting both
sides of the comparison to keywords present in both runs separates the two
events and allows a 0.9 threshold.

That restriction has a cost, and assertion 3 pays it: a truncated read that
returns only a small subset (for example, 10 of 100 keywords) leaves a shared
set of 10, both sides are restricted to those 10, and the yield ratio comes out
at roughly 1.0 -- it passes. A partial read is indistinguishable from a
deliberate deletion if you only look at the survivors. The size of the keyword
set must therefore be checked separately, and that check has to be blocking.
"""

import logging
from typing import Collection, Mapping, NamedTuple, Optional

# Reasons a run is refused. These are logged and returned, not raised, so the
# caller can report them without unpacking an exception.
ABORT_NO_KEYWORDS = 'no_usable_keywords'
ABORT_NO_DETECTIONS = 'staging_produced_no_detections'
ABORT_KEYWORD_SET_COLLAPSED = 'shared_keyword_set_empty'
ABORT_KEYWORD_SET_SHRANK = 'keyword_set_retention_below_threshold'
ABORT_YIELD_DROPPED = 'shared_keyword_yield_below_threshold'

DEFAULT_MIN_KEYWORD_RETENTION_RATIO = 0.9
DEFAULT_MIN_RETENTION_RATIO = 0.9

# A single keyword losing more than half its detections is reported but does
# not stop the run; it is a signal that one pattern may have regressed while
# the aggregate stayed healthy.
DEFAULT_PER_KEYWORD_WARN_RATIO = 0.5


class BreakerMetrics(NamedTuple):
  """Everything the breaker measured, whether or not it aborted.

  The ratios are recorded even when an earlier assertion has already failed,
  so that the reason for an abort can be distinguished from the assertions
  that would have passed.

  Attributes:
      keyword_count: Usable keywords in this run.
      previous_keyword_count: Distinct keywords that produced detections in the
        previous run.
      shared_keyword_count: Keywords common to both runs.
      keyword_retention_ratio: Shared keywords as a fraction of the previous
        run's keywords. None on a seeding run.
      staging_rows: Total detections produced by this run.
      previous_shared_rows: Previous detections attributable to shared keywords.
      staging_shared_rows: This run's detections attributable to shared
        keywords.
      yield_retention_ratio: staging_shared_rows over previous_shared_rows. None
        when it is undefined.
  """

  keyword_count: int
  previous_keyword_count: int
  shared_keyword_count: int
  keyword_retention_ratio: Optional[float]
  staging_rows: int
  previous_shared_rows: int
  staging_shared_rows: int
  yield_retention_ratio: Optional[float]


class BreakerDecision(NamedTuple):
  """The outcome of evaluating the breaker.

  Attributes:
      proceed: Whether the merge may run.
      reason: The abort reason constant, or None when proceeding.
      warnings: Non-blocking observations worth logging.
      metrics: What was measured.
      seeding_run: True when there was no previous run to compare against, in
        which case the comparative assertions were skipped.
  """

  proceed: bool
  reason: Optional[str]
  warnings: list[str]
  metrics: BreakerMetrics
  seeding_run: bool


def _ratio(numerator: float, denominator: float) -> Optional[float]:
  """Returns numerator / denominator, or None when it is undefined."""
  if denominator <= 0:
    return None
  return numerator / denominator


def evaluate(
    current_keywords: Collection[str],
    previous_counts: Mapping[str, int],
    staging_counts: Mapping[str, int],
    ack_keyword_set_change: bool = False,
    min_keyword_retention_ratio: float = DEFAULT_MIN_KEYWORD_RETENTION_RATIO,
    min_retention_ratio: float = DEFAULT_MIN_RETENTION_RATIO,
    per_keyword_warn_ratio: float = DEFAULT_PER_KEYWORD_WARN_RATIO,
    logger: Optional[logging.Logger] = None,
) -> BreakerDecision:
  """Decides whether this run's detections may replace the previous ones.

  Args:
      current_keywords: Keywords that survived validation in this run. This is
        the full usable list, not only the keywords that matched something.
      previous_counts: Detections per keyword recorded by the previous run.
        Empty on the first run.
      staging_counts: Detections per keyword produced by this run.
      ack_keyword_set_change: Set by a human triggering a manual run after a
        deliberate bulk edit to the keyword sheet. Waives the keyword-set
        assertion for this run only. It does not waive the yield assertion: a
        bulk keyword edit is no reason to accept a collapse in the yield of the
        keywords that remain.
      min_keyword_retention_ratio: Threshold for the keyword-set assertion.
      min_retention_ratio: Threshold for the yield assertion.
      per_keyword_warn_ratio: Below this, an individual keyword's drop is
        reported as a warning.
      logger: Optional logger.

  Returns:
      A BreakerDecision.
  """
  log = logger or logging.getLogger(__name__)
  warnings: list[str] = []

  keywords = set(current_keywords)
  previous_keywords = set(previous_counts)
  shared = keywords & previous_keywords

  staging_rows = sum(staging_counts.values())
  previous_shared_rows = sum(previous_counts[key] for key in shared)
  staging_shared_rows = sum(staging_counts.get(key, 0) for key in shared)

  metrics = BreakerMetrics(
      keyword_count=len(keywords),
      previous_keyword_count=len(previous_keywords),
      shared_keyword_count=len(shared),
      keyword_retention_ratio=_ratio(len(shared), len(previous_keywords)),
      staging_rows=staging_rows,
      previous_shared_rows=previous_shared_rows,
      staging_shared_rows=staging_shared_rows,
      yield_retention_ratio=_ratio(staging_shared_rows, previous_shared_rows),
  )

  def _abort(reason: str) -> BreakerDecision:
    log.error(
        'Circuit breaker aborted the run: %s. Metrics: %s', reason, metrics
    )
    return BreakerDecision(
        proceed=False,
        reason=reason,
        warnings=warnings,
        metrics=metrics,
        seeding_run=False,
    )

  # Assertion 1. Nothing to match with.
  if not keywords:
    return _abort(ABORT_NO_KEYWORDS)

  # Assertion 2. A well-formed keyword list matching nothing at all across
  # millions of entities is not a credible outcome.
  if staging_rows <= 0:
    return _abort(ABORT_NO_DETECTIONS)

  # No baseline to compare against. Explicit branch rather than an accidental
  # division by zero, and deliberately permissive so that an empty detection
  # table can be seeded on initial deployment.
  if not previous_keywords:
    log.info(
        'No previous run found; seeding %d detections without comparison.',
        staging_rows,
    )
    return BreakerDecision(
        proceed=True,
        reason=None,
        warnings=warnings,
        metrics=metrics,
        seeding_run=True,
    )

  # Assertion 3a. No overlap at all with the previous keyword set. This is what
  # a wholesale corrupted read looks like, and it also leaves the yield
  # assertion undefined, so it cannot be waived.
  if not shared:
    return _abort(ABORT_KEYWORD_SET_COLLAPSED)

  # Assertion 3b. The keyword set itself shrank. This is the only assertion
  # that catches a truncated read, because the yield assertion below is blind
  # to it by construction.
  if metrics.keyword_retention_ratio < min_keyword_retention_ratio:
    if not ack_keyword_set_change:
      return _abort(ABORT_KEYWORD_SET_SHRANK)
    warnings.append(
        'Keyword set retention {:.3f} is below {:.3f} but the run carries an '
        'explicit acknowledgement.'.format(
            metrics.keyword_retention_ratio, min_keyword_retention_ratio
        )
    )

  # Assertion 4. The keywords that survived both runs stopped finding what they
  # used to find. Not waivable.
  if (
      metrics.yield_retention_ratio is not None
      and metrics.yield_retention_ratio < min_retention_ratio
  ):
    return _abort(ABORT_YIELD_DROPPED)

  # Assertion 5. Advisory, per keyword.
  for keyword in sorted(shared):
    previous = previous_counts[keyword]
    current = staging_counts.get(keyword, 0)
    keyword_ratio = _ratio(current, previous)
    if keyword_ratio is not None and keyword_ratio < per_keyword_warn_ratio:
      warnings.append(
          'Keyword {!r} fell from {:d} to {:d} detections.'.format(
              keyword, previous, current
          )
      )

  for warning in warnings:
    log.warning('%s', warning)

  return BreakerDecision(
      proceed=True,
      reason=None,
      warnings=warnings,
      metrics=metrics,
      seeding_run=False,
  )
