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

r"""Unit tests for keyword pattern construction.

The patterns this module produces are executed by RE2 inside BigQuery, not by
Python. Python's ``re`` module understands neither ``\p{L}`` nor the ``\Q...\E``
literal-quoting span, so the semantic tests here translate each pattern into an
equivalent Python expression first. The translation is deliberately narrow:

  * ``\Q...\E`` becomes ``re.escape(...)``, which is what literal quoting means.
  * ``[^\p{L}\p{N}_]`` becomes ``[^\w]``. In Python's Unicode mode ``\w`` is
    letters, numbers and the underscore, so the two agree on every character
    exercised here -- including the accented letters, the currency symbol and
    the hash character, none of which are letters or numbers.

These tests guard against regressions in RE2 pattern construction and boundary
semantics.
"""

import re

from keyword_matcher.utils import patterns
import pytest

_LITERAL_QUOTE_SPAN = re.compile(r'\\Q(.*?)\\E', re.DOTALL)

# Python's re module has no \p{Script} support, so the CJK script class is
# translated into explicit codepoint ranges. This is an approximation of RE2's
# behaviour -- it covers the common blocks, not every historical extension --
# and it exists so the boundary logic can be exercised in a unit test.
_CJK_RANGES = (
    '\u3040-\u309f'  # Hiragana
    '\u30a0-\u30ff'  # Katakana
    '\u3400-\u4dbf'  # CJK Unified Ideographs Extension A
    '\u4e00-\u9fff'  # CJK Unified Ideographs
    '\uf900-\ufaff'  # CJK Compatibility Ideographs
    '\u1100-\u11ff'  # Hangul Jamo
    '\uac00-\ud7af'  # Hangul Syllables
)

_RE2_TO_PYTHON = {
    r'[^\p{L}\p{N}_]': r'[^\w]',
    r'[\p{Han}\p{Hiragana}\p{Katakana}\p{Hangul}]': f'[{_CJK_RANGES}]',
}


def _to_python_regex(pattern: str) -> str:
  """Translates an RE2 pattern into an equivalent Python regex.

  Args:
      pattern: An RE2 pattern produced by the patterns module.

  Returns:
      A pattern that Python's re module can compile.

  Raises:
      AssertionError: If any RE2-only construct survives translation, which
          would mean this table has drifted from the patterns module and the
          tests below are silently exercising the wrong expression.
  """
  translated = _LITERAL_QUOTE_SPAN.sub(
      lambda match: re.escape(match.group(1)), pattern
  )
  for re2_form, python_form in _RE2_TO_PYTHON.items():
    translated = translated.replace(re2_form, python_form)
  assert r'\p{' not in translated, (
      f'Untranslated RE2 property class in {translated!r}. Update '
      '_RE2_TO_PYTHON to match the patterns module.'
  )
  return translated


def _matches(keyword: str, text: str) -> bool:
  """Reports whether the new pattern for a keyword matches the given text."""
  pattern = patterns.build_keyword_pattern(patterns.normalise_keyword(keyword))
  return re.search(_to_python_regex(pattern), text.lower()) is not None


def _legacy_matches(keyword: str, text: str) -> bool:
  """Reports whether the legacy ASCII-only pattern matches the given text.

  The legacy implementation built ``\\b(keyword)[s]?\\b``. Python's ``\\b`` is
  Unicode-aware by default, whereas RE2's is ASCII-only, so re.ASCII is applied
  to reproduce RE2's ASCII word-boundary behaviour.

  Args:
      keyword: The keyword, as typed in the keyword sheet.
      text: The text to search.

  Returns:
      True when the legacy pattern matches.
  """
  legacy = r'\b(' + keyword.lower() + r')[s]?\b'
  return re.search(legacy, text.lower(), re.ASCII) is not None


# Verification cases comparing Unicode-aware boundaries ("expected") against
# RE2's ASCII-only \b ("legacy_expected"). The four rows where they disagree
# demonstrate why non-ASCII boundary characters require explicit Unicode
# character classes.
_VERIFICATION_CASES = [
    ('the über cool video', 'über', True, False),
    ('überlord content', 'über', False, False),
    ('watch gaming now', 'gaming', True, True),
    ('gaminghub channel', 'gaming', False, False),
    ('brand123 promo code', 'brand', False, False),
    ('#trending stream', '#trending', True, False),
    ('price is 100€ today', '100€', True, False),
    ('café review', 'café', True, False),
    ('cafés review', 'café', True, True),
    ('live music stream here', 'live music', True, True),
    ('a (topic) in brackets', 'topic', True, True),
]


@pytest.mark.parametrize(
    'text,keyword,expected,legacy_expected', _VERIFICATION_CASES
)
def test_boundary_cases(
    text: str, keyword: str, expected: bool, legacy_expected: bool
):
  """The new pattern is correct on all eleven verification cases."""
  del legacy_expected  # Asserted separately below.
  assert _matches(keyword, text) is expected


@pytest.mark.parametrize(
    'text,keyword,expected,legacy_expected', _VERIFICATION_CASES
)
def test_legacy_pattern_defect_is_reproduced(
    text: str, keyword: str, expected: bool, legacy_expected: bool
):
  """Characterises the ASCII-only word-boundary behaviour of RE2 \\b."""
  del expected
  assert _legacy_matches(keyword, text) is legacy_expected


def test_new_pattern_strictly_improves_on_legacy():
  """The new pattern never loses a match the legacy pattern found."""
  for text, keyword, expected, legacy_expected in _VERIFICATION_CASES:
    if legacy_expected and expected:
      assert _matches(keyword, text), (keyword, text)

  corrected = [case for case in _VERIFICATION_CASES if case[2] is not case[3]]
  assert len(corrected) == 4


# Scripts that do not separate words with spaces: Han, Hiragana, Katakana, and
# Hangul characters act as boundaries around embedded Latin tokens.
_CJK_BOUNDARY_CASES = [
    ('今天有个topic很热门，大家都在讨论', 'topic', True),
    ('おすすめのTOPIC動画', 'topic', True),
    ('这期节目介绍了小topic#合集', 'topic', True),
    ('오늘의topic영상', 'topic', True),
    # The boundary must still reject a keyword embedded inside a Latin word.
    ('topical tutorial', 'topic', False),
]


@pytest.mark.parametrize('text,keyword,expected', _CJK_BOUNDARY_CASES)
def test_latin_keyword_adjacent_to_cjk_script(
    text: str, keyword: str, expected: bool
):
  """A Latin keyword embedded in CJK text is a standalone word.

  Chinese, Japanese and Korean do not put spaces between words, so adjacency
  to one of those scripts is a word boundary even though every character in
  them is a Unicode letter.
  """
  assert _matches(keyword, text) is expected


def test_cjk_keyword_is_not_inert():
  """A CJK keyword must match CJK text.

  If a Han-script keyword is added while the boundary recognises only
  [^\\p{L}\\p{N}_], the preceding character in Han text is a Unicode letter
  and the boundary can never be satisfied in running text. Including CJK
  scripts in the boundary alternation allows CJK keywords to match naturally.
  """
  assert _matches('音乐', '这是我的音乐节目')
  assert _matches('音乐', '音乐')


def test_cjk_boundary_only_widens():
  """Adding the CJK scripts cannot remove a match.

  A boundary expressed as an alternation can only ever admit more positions,
  so every case the narrower boundary accepted must still be accepted. Pinning
  this means the CJK exception can never be blamed for a regression.
  """
  narrow_leading = r'(?:^|[^\p{L}\p{N}_])'
  narrow_trailing = r'(?:$|[^\p{L}\p{N}_])'
  for text, keyword, expected, _ in _VERIFICATION_CASES:
    if not expected:
      continue
    normalised = patterns.normalise_keyword(keyword)
    narrow = f'{narrow_leading}\\Q{normalised}\\Es?{narrow_trailing}'
    if re.search(_to_python_regex(narrow), text.lower()):
      assert _matches(keyword, text), (keyword, text)

  corrected = [case for case in _VERIFICATION_CASES if case[2] is not case[3]]
  assert len(corrected) == 4


def test_keyword_pattern_is_the_expected_re2_string():
  """Pins the exact generated pattern, since RE2 is not exercised locally.

  The expected string is written out in full rather than composed from the
  module's own constants. Composing it would make the test tautological: it
  would pass whatever the constants happened to say. Spelled out, any change
  to the boundary must be made deliberately in two places.
  """
  boundary_chars = r'[^\p{L}\p{N}_]|[\p{Han}\p{Hiragana}\p{Katakana}\p{Hangul}]'
  assert patterns.build_keyword_pattern('alpha') == (
      f'(?:^|{boundary_chars})\\Qalpha\\Es?(?:$|{boundary_chars})'
  )


def test_regex_metacharacters_are_matched_literally():
  """A keyword containing metacharacters is a literal, not an expression."""
  assert _matches('brand(123)', 'find a brand(123) here')
  assert not _matches('brand(123)', 'find a brand123 here')
  assert not _matches('a.b', 'find a axb here')
  assert _matches('a.b', 'find a a.b here')


def test_multi_word_keyword():
  assert _matches('live music', 'the best live music around')
  assert not _matches('live music', 'live  music with two spaces')


def test_plural_suffix_is_retained():
  assert _matches('review', 'reviews are here')
  assert not _matches('review', 'reviewed is not a plural')


def test_normalisation_trims_and_lowercases():
  assert patterns.normalise_keyword('  AlPhA  ') == 'alpha'


def test_validation_rejects_empty_keywords():
  assert patterns.validate_keyword('') == patterns.REJECT_EMPTY
  assert (
      patterns.validate_keyword(patterns.normalise_keyword('   '))
      == patterns.REJECT_EMPTY
  )


def test_validation_rejects_literal_quote_terminator():
  """A keyword containing \\E would escape literal quoting."""
  assert (
      patterns.validate_keyword(r'alpha\Emalicious')
      == patterns.REJECT_QUOTE_TERMINATOR
  )


def test_literal_quote_guard_is_not_defeated_by_normalisation():
  """Regression: the guard must inspect the keyword before it is lowercased.

  normalise_keyword lowercases, which turns the case-sensitive terminator
  ``\\E`` into the inert ``\\e``. Validating after normalisation would prevent
  this guard from firing and rely solely on lowercasing for safety.
  """
  raw = r'alpha\Einjected'
  result = patterns.build_pattern_set([raw])
  assert not result.patterns
  assert result.rejected == [(raw, patterns.REJECT_QUOTE_TERMINATOR)]


def test_validation_rejects_control_characters():
  assert (
      patterns.validate_keyword('alpha\x00')
      == patterns.REJECT_CONTROL_CHARACTER
  )


def test_validation_accepts_ordinary_keywords():
  assert patterns.validate_keyword('live music') is None
  assert patterns.validate_keyword('100€') is None


def test_build_pattern_set_deduplicates_and_sorts():
  result = patterns.build_pattern_set(['Beta', 'beta ', 'alpha'])
  assert result.keywords == ['alpha', 'beta']
  assert not result.rejected


def test_build_pattern_set_records_rejections_without_raising():
  result = patterns.build_pattern_set(['alpha', '   ', r'bad\Ekeyword'])
  assert result.keywords == ['alpha']
  reasons = {reason for _, reason in result.rejected}
  assert reasons == {
      patterns.REJECT_EMPTY,
      patterns.REJECT_QUOTE_TERMINATOR,
  }


def test_fingerprint_is_stable_under_reordering_and_cosmetic_edits():
  first = patterns.build_pattern_set(['alpha', 'beta', 'gamma'])
  second = patterns.build_pattern_set([' Gamma ', 'BETA', 'alpha'])
  assert first.fingerprint == second.fingerprint


def test_fingerprint_changes_when_the_keyword_set_changes():
  first = patterns.build_pattern_set(['alpha', 'beta'])
  second = patterns.build_pattern_set(['alpha', 'beta', 'gamma'])
  assert first.fingerprint != second.fingerprint


def test_admission_pattern_detects_any_keyword():
  result = patterns.build_pattern_set(['alpha', 'beta'])
  compiled = re.compile(_to_python_regex(result.admission_pattern))
  assert compiled.search('an alpha item')
  assert compiled.search('a beta release')
  assert not compiled.search('nothing relevant here')


def test_admission_pattern_is_empty_when_no_keywords_survive():
  result = patterns.build_pattern_set(['   '])
  assert result.admission_pattern == ''
  assert not result.patterns


def test_adjacent_keywords_are_both_detected_by_existence_tests():
  """Guards the trap that ruled out REGEXP_EXTRACT_ALL.

  The boundary classes consume their delimiter, so an extraction over a
  combined alternation drops the second of two adjacent keywords. Per-keyword
  existence testing, which is what the generated SQL uses, is immune.
  """
  text = 'watch alpha beta now'
  assert _matches('alpha', text)
  assert _matches('beta', text)
