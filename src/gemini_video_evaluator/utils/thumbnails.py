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

"""YouTube thumbnail URL construction and concurrent availability probing.

Empirical contract verified across 2,000 VET URLs and 68 Agent Platform Batch
evaluations:
  1. Agent Platform Batch Inference fetches
  `https://i.ytimg.com/vi/{id}/{slot}.jpg`
     directly via `fileData.fileUri` without needing a GCS staging bucket.
  2. However, if even a SINGLE URL in a multi-image `GenerateContentRequest`
     returns HTTP 404, the remote image fetcher aborts the entire video
     row with `URL_ERROR-ERROR_NOT_FOUND`.
  3. YouTube's CDN (`i.ytimg.com`) also returns a 1,073-byte gray 120x90 JPEG
     placeholder on missing thumbnails. Therefore, a thumbnail URL is considered
     valid if and only if HTTP status is 200 and `len(body) > min_bytes`
     (default 1,500 bytes).
"""

from concurrent import futures
import dataclasses
from typing import Callable, Optional, Sequence
import requests
from requests import adapters

# A function `(url, timeout_seconds) -> (status_code, content_len)`.
UrlProbeFn = Callable[[str, float], tuple[int, int]]


@dataclasses.dataclass(frozen=True)
class ThumbnailProbeResult:
  """Result of probing candidate thumbnail URLs for a single YouTube video."""

  video_id: str
  verified_urls: tuple[str, ...]
  slots_probed: int
  slots_valid: int


def build_candidate_urls(
    video_id: str,
    resolution: str = 'hq',
    slots: Sequence[str] = ('default', '1', '2', '3'),
) -> list[str]:
  """Constructs candidate `https://i.ytimg.com/vi/...` URLs for a video."""
  vid = str(video_id).strip()
  if not vid:
    raise ValueError('video_id must not be empty')
  prefix = str(resolution).strip()
  return [f'https://i.ytimg.com/vi/{vid}/{prefix}{slot}.jpg' for slot in slots]


def _make_session_prober(pool_size: int) -> tuple[requests.Session, UrlProbeFn]:
  """Creates a pooled `requests.Session` and a `UrlProbeFn` that uses it."""
  session = requests.Session()
  adapter = adapters.HTTPAdapter(
      pool_connections=pool_size, pool_maxsize=pool_size, max_retries=1
  )
  session.mount('https://', adapter)
  session.mount('http://', adapter)

  def _fetch(url: str, timeout_seconds: float) -> tuple[int, int]:
    try:
      resp = session.get(url, timeout=timeout_seconds)
      return resp.status_code, len(resp.content)
    except requests.RequestException:
      return 0, 0

  return session, _fetch


def probe_video_thumbnails(
    video_ids: Sequence[str],
    resolution: str = 'hq',
    slots: Sequence[str] = ('default', '1', '2', '3'),
    min_bytes: int = 1500,
    concurrency: int = 32,
    timeout_seconds: float = 5.0,
    probe_fn: Optional[UrlProbeFn] = None,
) -> dict[str, ThumbnailProbeResult]:
  """Probes all candidate thumbnail URLs for `video_ids` concurrently."""
  unique_ids = list(dict.fromkeys(str(v).strip() for v in video_ids if v))
  if not unique_ids:
    return {}

  max_workers = max(1, concurrency)
  session, active_prober = (
      _make_session_prober(max_workers)
      if probe_fn is None
      else (None, probe_fn)
  )

  def _is_valid(url: str) -> bool:
    status_code, byte_len = active_prober(url, timeout_seconds)
    return status_code == 200 and byte_len > min_bytes

  def _probe_one_video(vid: str) -> ThumbnailProbeResult:
    urls = build_candidate_urls(vid, resolution=resolution, slots=slots)
    if not urls:
      return ThumbnailProbeResult(vid, (), slots_probed=0, slots_valid=0)
    # Probe the primary cover slot first: when slot 0 returns HTTP 404 (a
    # deleted or private video), secondary storyboard slots ('1', '2', '3') are
    # also absent, so short-circuiting here saves 3 requests per dead video.
    if not _is_valid(urls[0]):
      return ThumbnailProbeResult(vid, (), slots_probed=1, slots_valid=0)
    verified = (urls[0], *(u for u in urls[1:] if _is_valid(u)))
    return ThumbnailProbeResult(
        vid, verified, slots_probed=len(urls), slots_valid=len(verified)
    )

  try:
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
      return {r.video_id: r for r in executor.map(_probe_one_video, unique_ids)}
  finally:
    if session is not None:
      session.close()
