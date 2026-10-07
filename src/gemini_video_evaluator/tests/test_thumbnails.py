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

"""Unit tests for gemini_video_evaluator.utils.thumbnails."""

from gemini_video_evaluator.utils import thumbnails
import pytest


def test_build_candidate_urls_resolutions():
  """Verifies URL construction for hq, maxres, and bare prefix."""
  hq_urls = thumbnails.build_candidate_urls('vid_test_001', resolution='hq')
  assert hq_urls == [
      'https://i.ytimg.com/vi/vid_test_001/hqdefault.jpg',
      'https://i.ytimg.com/vi/vid_test_001/hq1.jpg',
      'https://i.ytimg.com/vi/vid_test_001/hq2.jpg',
      'https://i.ytimg.com/vi/vid_test_001/hq3.jpg',
  ]

  bare_urls = thumbnails.build_candidate_urls(
      'vid_test_001', resolution='', slots=('default', '1')
  )
  assert bare_urls == [
      'https://i.ytimg.com/vi/vid_test_001/default.jpg',
      'https://i.ytimg.com/vi/vid_test_001/1.jpg',
  ]

  with pytest.raises(ValueError, match='video_id must not be empty'):
    thumbnails.build_candidate_urls('   ')


def test_probe_video_thumbnails_filters_404_and_gray_placeholder():
  """Verifies 404s and 1073-byte placeholders are excluded.

  Also verifies that a dead video stops after its first slot.
  """
  requested_urls: list[str] = []

  def _fake_probe(url: str, timeout: float) -> tuple[int, int]:
    del timeout
    requested_urls.append(url)
    if '/live_all_4/' in url:
      return 200, 12000
    if '/live_partial/' in url:
      if url.endswith('/hq2.jpg'):
        # A 404 on slot 2 would fail the whole row in Agent Platform Batch.
        return 404, 1073
      if url.endswith('/hq3.jpg'):
        # Simulate HTTP 200 with 1073-byte gray error placeholder
        return 200, 1073
      return 200, 9500
    if '/dead_video/' in url:
      return 404, 1073
    return 500, 0

  results = thumbnails.probe_video_thumbnails(
      video_ids=['live_all_4', 'live_partial', 'dead_video'],
      resolution='hq',
      slots=('default', '1', '2', '3'),
      min_bytes=1500,
      probe_fn=_fake_probe,
  )

  # 1. Live video with all 4 valid thumbnails
  assert results['live_all_4'].slots_probed == 4
  assert results['live_all_4'].slots_valid == 4
  assert len(results['live_all_4'].verified_urls) == 4

  # 2. Live video with one 404 and one 1073-byte placeholder: 2 verified URLs
  assert results['live_partial'].slots_probed == 4
  assert results['live_partial'].slots_valid == 2
  assert results['live_partial'].verified_urls == (
      'https://i.ytimg.com/vi/live_partial/hqdefault.jpg',
      'https://i.ytimg.com/vi/live_partial/hq1.jpg',
  )

  # 3. Dead video short-circuits after slot 0 ('hqdefault.jpg') fails
  assert results['dead_video'].slots_probed == 1
  assert results['dead_video'].slots_valid == 0
  assert not results['dead_video'].verified_urls
  dead_requests = [u for u in requested_urls if '/dead_video/' in u]
  assert dead_requests == ['https://i.ytimg.com/vi/dead_video/hqdefault.jpg']
