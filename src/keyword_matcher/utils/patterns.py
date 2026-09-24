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

r"""Keyword pattern construction and validation for the keyword detector.

This module owns every decision about what counts as a keyword match. It
performs no input or output, so the rules can be unit tested without a
BigQuery client.

Why explicit Unicode boundaries are required: wrapping a keyword as
``\b(keyword)[s]?\b`` in RE2 (the regular expression engine BigQuery uses)
relies on ``\b``, which is ASCII-only -- it asserts a transition between
``[A-Za-z0-9_]`` and anything else. A keyword whose first or last character is
not an ASCII word character therefore has its boundary assertion inverted: it
demands an adjacent word character where a delimiter should be, and so can only
match when the surrounding text happens to append an ASCII character to it. The
keyword ``cafe`` with an acute accent fails on "cafe review" but succeeds on
"cafes review", the only difference being the trailing ASCII ``s``.

This module states the boundary explicitly with Unicode character classes,
and quotes the keyword literally so that regex metacharacters typed into the
keyword sheet are matched as themselves.
"""

import hashlib
import logging
from typing import Iterable, NamedTuple, Optional
import unicodedata

# What counts as "inside a word": any Unicode letter, any Unicode number, or
# the underscore. This is the Unicode-aware replacement for RE2's ASCII-only
# \b assertion.
_WORD_CHARS = r'\p{L}\p{N}_'

# Scripts that do not separate words with spaces. Every character in them is a
# Unicode letter, so without this exception a Latin keyword embedded in Chinese,
# Japanese, or Korean text would fail the boundary test despite being a
# standalone word (for example, "かわいいTOPIC" or "오늘의topic영상").
#
# The exception is also what makes a CJK keyword matchable in running text. If a
# Han-script keyword such as "音乐" is added to the keyword sheet, the character
# preceding it in Han text is a Unicode letter; without this script alternation,
# the boundary would never be satisfied in unspaced prose.
_CJK_SCRIPTS = r'\p{Han}\p{Hiragana}\p{Katakana}\p{Hangul}'

# A word boundary written as an explicit character class. Unlike \b, which is a
# zero-width assertion, these alternatives *consume* the delimiter character.
# That difference is harmless for existence tests and harmful for extraction --
# see the warning on build_admission_pattern.
#
# Widening a boundary can only ever add matches, never remove them, so this is
# a strict superset of the space-separated-script behaviour.
_LEADING_BOUNDARY = rf'(?:^|[^{_WORD_CHARS}]|[{_CJK_SCRIPTS}])'
_TRAILING_BOUNDARY = rf'(?:$|[^{_WORD_CHARS}]|[{_CJK_SCRIPTS}])'

# RE2 literal-quoting delimiters. Everything between \Q and \E is matched
# literally, which is what neutralises regex metacharacters in keywords.
_QUOTE_OPEN = r'\Q'
_QUOTE_CLOSE = r'\E'

# Optional English plural suffix retained for backwards compatibility with
# existing keyword sheet conventions.
_PLURAL_SUFFIX = 's?'

# Rejection reasons returned by validate_keyword.
REJECT_EMPTY = 'empty_after_trimming'
REJECT_QUOTE_TERMINATOR = 'contains_literal_quote_terminator'
REJECT_CONTROL_CHARACTER = 'contains_control_character'


class KeywordPattern(NamedTuple):
  """A single keyword and the RE2 pattern that detects it.

  Attributes:
      keyword: The normalised keyword, used as the detection label.
      pattern: The RE2 pattern to evaluate against lowercased text.
  """

  keyword: str
  pattern: str


class PatternSet(NamedTuple):
  """The outcome of turning a raw keyword list into usable patterns.

  Attributes:
      patterns: Accepted keywords with their per-keyword patterns, sorted by
        keyword so that generated queries are deterministic.
      admission_pattern: A single combined pattern used to cheaply reduce the
        corpus before attribution. Empty when no keyword was accepted.
      rejected: Pairs of (raw keyword, rejection reason) for keywords that
        failed validation.
      fingerprint: A stable hash of the accepted keyword set.
  """

  patterns: list[KeywordPattern]
  admission_pattern: str
  rejected: list[tuple[str, str]]
  fingerprint: str

  @property
  def keywords(self) -> list[str]:
    """Returns the accepted keywords, in the same order as the patterns."""
    return [item.keyword for item in self.patterns]


def normalise_keyword(keyword: str) -> str:
  """Trims and lowercases a keyword.

  Normalising before both hashing and pattern construction means cosmetic edits
  to the keyword sheet -- a stray trailing space, a change of case -- do not
  alter behaviour or invalidate the fingerprint.

  Args:
      keyword: The raw keyword as read from the keyword sheet.

  Returns:
      The normalised keyword.
  """
  return keyword.strip().lower()


def validate_keyword(keyword: str) -> Optional[str]:
  """Checks whether a keyword can safely be used to build a pattern.

  This must be called on the *raw* keyword, before normalise_keyword. The
  literal-quote terminator is case-sensitive: lowercasing turns ``\\E`` into
  ``\\e``, which is inert, so validating after normalisation would mean this
  guard never fires. It would appear to work, because that particular payload
  is defused by accident, and would stop working the moment normalisation
  changed. Emptiness is therefore checked against the trimmed keyword here
  rather than being deferred to the caller.

  Args:
      keyword: A keyword exactly as read from the keyword sheet.

  Returns:
      A rejection reason constant, or None when the keyword is usable.
  """
  # A keyword containing the literal-quote terminator would close the \Q...\E
  # span early and let the remainder of the keyword be interpreted as a regular
  # expression. That is the exact injection this quoting exists to prevent.
  if _QUOTE_CLOSE in keyword:
    return REJECT_QUOTE_TERMINATOR
  if any(unicodedata.category(character) == 'Cc' for character in keyword):
    return REJECT_CONTROL_CHARACTER
  if not keyword.strip():
    return REJECT_EMPTY
  return None


def build_keyword_pattern(keyword: str) -> str:
  """Builds the RE2 pattern that detects one keyword.

  Args:
      keyword: A normalised keyword that has passed validate_keyword.

  Returns:
      An RE2 pattern to be evaluated against lowercased text.
  """
  return (
      f'{_LEADING_BOUNDARY}{_QUOTE_OPEN}{keyword}{_QUOTE_CLOSE}'
      f'{_PLURAL_SUFFIX}{_TRAILING_BOUNDARY}'
  )


def build_admission_pattern(keywords: Iterable[str]) -> str:
  """Builds one combined pattern that detects any of the keywords.

  This is used only to reduce the corpus to entities worth attributing, as a
  single pass that answers "does this text contain any keyword at all".

  WARNING: this combined pattern is safe for existence tests
  (REGEXP_CONTAINS) and MUST NOT be used with REGEXP_EXTRACT_ALL. The boundary
  classes above consume the delimiter character, unlike the zero-width \\b they
  replace, and RE2 resumes scanning after the end of the previous match. Two
  keywords separated by a single delimiter therefore cannot both be extracted:
  REGEXP_EXTRACT_ALL over "watch alpha beta now" returns only ['alpha'] and
  loses 'beta', while "watch alpha and then beta now" returns both. Non-adjacent
  keywords behave correctly, which is precisely why this fault would survive
  casual testing. Attribution uses per-keyword REGEXP_CONTAINS instead, which
  is immune because it never resumes scanning.

  Args:
      keywords: Normalised keywords that have passed validate_keyword.

  Returns:
      A combined RE2 pattern, or the empty string if no keywords were given.
  """
  quoted = [f'{_QUOTE_OPEN}{keyword}{_QUOTE_CLOSE}' for keyword in keywords]
  if not quoted:
    return ''
  return (
      f'{_LEADING_BOUNDARY}(?:{"|".join(quoted)})'
      f'{_PLURAL_SUFFIX}{_TRAILING_BOUNDARY}'
  )


def fingerprint_keywords(keywords: Iterable[str]) -> str:
  """Hashes a keyword set in a way that is stable under reordering.

  Args:
      keywords: Normalised keywords.

  Returns:
      A hexadecimal SHA-256 digest of the canonicalised keyword set.
  """
  canonical = '\n'.join(sorted(set(keywords)))
  return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def build_pattern_set(
    raw_keywords: Iterable[str],
    logger: Optional[logging.Logger] = None,
) -> PatternSet:
  """Normalises, validates and compiles a raw keyword list into patterns.

  Duplicates that arise from normalisation -- for example "Alpha" and "alpha "
  -- collapse into one keyword.

  This function never raises on a bad keyword; it drops it and records the
  reason. Deciding whether the surviving set is large enough to act on belongs
  to the circuit breaker, which can compare it against the previous run.

  Args:
      raw_keywords: Keywords exactly as read from the keyword sheet.
      logger: Optional logger for reporting rejected keywords.

  Returns:
      A PatternSet describing what was accepted and what was rejected.
  """
  log = logger or logging.getLogger(__name__)

  accepted: set[str] = set()
  rejected: list[tuple[str, str]] = []

  for raw_keyword in raw_keywords:
    # Validation precedes normalisation deliberately; see validate_keyword.
    reason = validate_keyword(raw_keyword)
    if reason is None:
      accepted.add(normalise_keyword(raw_keyword))
      continue
    rejected.append((raw_keyword, reason))
    if reason == REJECT_QUOTE_TERMINATOR:
      log.error('Rejected keyword %r: %s', raw_keyword, reason)
    else:
      log.warning('Rejected keyword %r: %s', raw_keyword, reason)

  ordered = sorted(accepted)
  patterns = [
      KeywordPattern(keyword=keyword, pattern=build_keyword_pattern(keyword))
      for keyword in ordered
  ]

  log.info(
      'Compiled %d keyword patterns, rejected %d.', len(patterns), len(rejected)
  )

  return PatternSet(
      patterns=patterns,
      admission_pattern=build_admission_pattern(ordered),
      rejected=rejected,
      fingerprint=fingerprint_keywords(ordered),
  )
