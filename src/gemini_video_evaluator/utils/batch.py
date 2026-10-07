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

"""Batch Inference requests, job state, and parsing of Gemini's answers."""

import dataclasses
import json
import math
from typing import Any, Mapping, Optional, Sequence
from gemini_video_evaluator.utils import config as config_lib
from gemini_video_evaluator.utils import prompt
from gemini_video_evaluator.utils import sql

DISPLAY_NAME_PREFIX = 'vet-gemini-video-eval-'
INPUT_TABLE_PREFIX = '_gemini_eval_input_'
OUTPUT_TABLE_PREFIX = '_gemini_eval_output_'
STAGING_TABLE_PREFIX = '_gemini_eval_staging_'

ACTIVE_JOB_STATES = frozenset({
    'JOB_STATE_PENDING',
    'JOB_STATE_QUEUED',
    'JOB_STATE_RUNNING',
    'JOB_STATE_UPDATING',
    'JOB_STATE_CANCELLING',
})

# Both leave an output table behind. Rows that failed inside a partially
# successful job carry a status and are handled one by one, so the rows that
# did succeed are not thrown away with them.
SUCCEEDED_JOB_STATES = frozenset({
    'JOB_STATE_SUCCEEDED',
    'JOB_STATE_PARTIALLY_SUCCEEDED',
})

FAILED_JOB_STATES = frozenset({
    'JOB_STATE_FAILED',
    'JOB_STATE_CANCELLED',
    'JOB_STATE_EXPIRED',
    'JOB_STATE_PAUSED',
})

# The threshold in force before it was stored with each request. Only batches
# sent by earlier versions lack the max_minor_age column.
_LEGACY_MAX_MINOR_AGE = 18


@dataclasses.dataclass(frozen=True)
class DetectionRecord:
  """One detection row, ready to be staged and merged into `detection`."""

  entity_type: str
  entity_id: str
  modality: str
  source: str
  detector: str
  detector_version: str
  taxonomy: str
  label: str
  score: float
  evidence: str  # JSON-serialized string


@dataclasses.dataclass(frozen=True)
class ParsedVideoEvaluation:
  """Result of parsing a single Agent Platform Batch Inference output row."""

  video_id: str
  detector_version: str
  succeeded: bool
  error_message: Optional[str]
  detections: tuple[DetectionRecord, ...]


@dataclasses.dataclass(frozen=True)
class RequestedEvaluations:
  """Which checks a stored request asked Gemini for."""

  age_check: bool
  categories: tuple[str, ...]


def normalize_job_state(state: Any) -> str:
  """Returns a `JobState` enum or string as its name, 'JOB_STATE_...'."""
  if state is None:
    return 'JOB_STATE_UNSPECIFIED'
  enum_name = getattr(state, 'name', None)
  if isinstance(enum_name, str) and enum_name:
    return enum_name
  return str(state).rsplit('.', 1)[-1].strip()


def format_completion_stats(job: Any) -> str:
  """Formats a job's progress as `<done>/<total> (<ok> ok, <failed> failed)`."""
  # `successful_count` and `failed_count` are None before the first flush, and
  # `incomplete_count` is -1 when the denominator is unknown.
  stats = getattr(job, 'completion_stats', None)
  if stats is None:
    return '0/? (0 ok, 0 failed)'
  successful = int(getattr(stats, 'successful_count', None) or 0)
  failed = int(getattr(stats, 'failed_count', None) or 0)
  processed = successful + failed
  incomplete_raw = getattr(stats, 'incomplete_count', None)
  if incomplete_raw is not None and int(incomplete_raw) >= 0:
    return (
        f'{processed}/{processed + int(incomplete_raw)} ({successful} ok,'
        f' {failed} failed)'
    )
  return f'{processed}/? ({successful} ok, {failed} failed)'


def model_id(job: Any) -> str:
  """Returns the model a batch job ran, e.g. 'gemini-3.8-flash'."""
  # Read from the job rather than configuration, because the configured model
  # may have changed while the job was running.
  model = str(getattr(job, 'model', '') or '')
  return model.rsplit('/', 1)[-1] or 'unknown'


def parse_bq_uri(bq_uri: Optional[str]) -> Optional[tuple[str, str, str]]:
  """Parses `bq://project.dataset.table` into `(project, dataset, table)`."""
  if not bq_uri or not isinstance(bq_uri, str):
    return None
  parts = bq_uri.strip().removeprefix('bq://').split('.')
  if len(parts) != 3 or not all(parts):
    return None
  return parts[0], parts[1], parts[2]


def extract_output_bq_table(job: Any) -> Optional[tuple[str, str, str]]:
  """Returns `(project, dataset, table)` of a job's `dest.bigquery_uri`."""
  dest = getattr(job, 'dest', None)
  return parse_bq_uri(getattr(dest, 'bigquery_uri', None) if dest else None)


def derive_companion_tables(output_table: str) -> tuple[str, str]:
  """Returns the input and staging tables that share an output table's tag."""
  tag = output_table.removeprefix(OUTPUT_TABLE_PREFIX)
  return f'{INPUT_TABLE_PREFIX}{tag}', f'{STAGING_TABLE_PREFIX}{tag}'


def is_our_batch_job(job: Any, project: str, dataset: str) -> bool:
  """Returns True if and only if `job` belongs to this VET project and dataset."""
  parsed = extract_output_bq_table(job)
  if parsed is None:
    return False
  dest_project, dest_dataset, dest_table = parsed
  display_name = str(getattr(job, 'display_name', '') or '')
  return (
      dest_project == project
      and dest_dataset == dataset
      and dest_table.startswith(OUTPUT_TABLE_PREFIX)
      and (not display_name or display_name.startswith(DISPLAY_NAME_PREFIX))
  )


def build_batch_input_row(
    video: Mapping[str, Any],
    verified_urls: Sequence[str],
    sheet_config: config_lib.SheetConfig,
    run_id: str,
) -> dict[str, Any]:
  """Constructs a single BigQuery Batch Inference input row for `video`."""
  # Only verified 200 OK thumbnail URLs (>1,500 bytes) are placed in `parts`
  # so a missing frame never triggers `URL_ERROR-ERROR_NOT_FOUND`. The row also
  # stores `request` (with `responseSchema`) and `max_minor_age` so Phase 1 can
  # collect the answer later without reading the Config Sheet.
  video_id = str(video['video_id']).strip()
  clean_urls = [str(u).strip() for u in verified_urls if u]

  parts: list[dict[str, Any]] = [
      {'fileData': {'fileUri': url, 'mimeType': 'image/jpeg'}}
      for url in clean_urls
  ]
  parts.append({
      'text': prompt.build_prompt(
          sheet_config=sheet_config,
          video=video,
          verified_image_count=len(clean_urls),
      )
  })

  request_payload: dict[str, Any] = {
      'contents': [{'role': 'user', 'parts': parts}],
      'generationConfig': {
          'temperature': prompt.TEMPERATURE,
          'mediaResolution': prompt.MEDIA_RESOLUTION,
          'responseMimeType': 'application/json',
          'responseSchema': prompt.build_response_schema(sheet_config),
          'thinkingConfig': {'thinkingBudget': prompt.THINKING_BUDGET},
      },
  }

  return {
      'video_id': video_id,
      'run_id': run_id,
      'detector_version': prompt.DETECTOR_VERSION,
      'max_minor_age': sheet_config.max_minor_age,
      'verified_thumbnail_count': len(clean_urls),
      'request': json.dumps(request_payload, ensure_ascii=False),
  }


def _json_mapping(value: Any) -> Optional[Mapping[str, Any]]:
  """Decodes a JSON string or returns `value` if it is already a Mapping."""
  if isinstance(value, str):
    try:
      value = json.loads(value)
    except json.JSONDecodeError:
      return None
  return value if isinstance(value, Mapping) else None


def _dig(obj: Any, *keys: str) -> Any:
  """Walks nested Mappings along `keys`, returning None if any step is missing."""
  for key in keys:
    if not isinstance(obj, Mapping):
      return None
    obj = obj.get(key)
  return obj


def requested_evaluations(request: Any) -> Optional[RequestedEvaluations]:
  """Reads which checks a stored request asked for from its response schema."""
  props = _dig(
      _json_mapping(request),
      'generationConfig',
      'responseSchema',
      'properties',
  )
  if not isinstance(props, Mapping):
    return None
  cat_props = _dig(props, 'categories', 'properties')
  categories = (
      tuple(str(name) for name in cat_props)
      if isinstance(cat_props, Mapping)
      else ()
  )
  age_check = 'persons' in props
  if not age_check and not categories:
    return None
  return RequestedEvaluations(age_check=age_check, categories=categories)


def _extract_response_json_text(response_obj: Any) -> Optional[str]:
  """Returns the first text part of a response that is not a thought."""
  candidates = _dig(_json_mapping(response_obj), 'candidates')
  if not isinstance(candidates, Sequence) or not isinstance(
      candidates, (list, tuple)
  ):
    return None
  if not candidates:
    return None
  parts = _dig(candidates[0], 'content', 'parts')
  if not isinstance(parts, (list, tuple)):
    return None
  for part in parts:
    if isinstance(part, Mapping) and not part.get('thought', False):
      text = part.get('text')
      if isinstance(text, str) and text.strip():
        return text.strip()
  return None


def _finite_number(value: Any) -> Optional[float]:
  """Returns a JSON number as a float, or None if it is not a finite number."""
  if isinstance(value, bool) or not isinstance(value, (int, float)):
    return None
  try:
    number = float(value)
  except OverflowError:
    return None
  return number if math.isfinite(number) else None


def _estimated_age(value: Any) -> Optional[int]:
  """Returns an estimated age in whole years, or None if it is unusable."""
  number = _finite_number(value)
  if number is None:
    return None
  age = int(number)
  return age if 0 <= age <= 120 else None


def _record(
    video_id: str,
    detector_version: str,
    modality: str,
    taxonomy: str,
    label: str,
    score: float,
    evidence: Mapping[str, Any],
) -> DetectionRecord:
  """Builds one detection row for this detector."""
  return DetectionRecord(
      entity_type=sql.ENTITY_VIDEO,
      entity_id=video_id,
      modality=modality,
      source=sql.SOURCE,
      detector=sql.DETECTOR,
      detector_version=detector_version,
      taxonomy=taxonomy,
      label=label,
      score=score,
      evidence=json.dumps(evidence, ensure_ascii=False),
  )


def _minor_presence_record(
    video_id: str,
    detector_version: str,
    persons: Sequence[Any],
    max_minor_age: int,
    model: str,
    verified_thumbnail_count: int,
    reasoning: str,
) -> DetectionRecord:
  """Scores 1.0 if the youngest real person is at most `max_minor_age`."""
  normalized_persons: list[dict[str, Any]] = []
  real_ages: list[int] = []
  for person in persons:
    if not isinstance(person, Mapping):
      continue
    raw_real = person.get('is_real_human')
    is_real = isinstance(raw_real, bool) and raw_real
    age = _estimated_age(person.get('estimated_age')) if is_real else None
    if age is not None:
      real_ages.append(age)
    normalized_persons.append({
        'description': str(person.get('description') or '').strip(),
        'is_real_human': is_real,
        'estimated_age': age,
    })

  real_human_count = sum(p['is_real_human'] for p in normalized_persons)
  youngest = min(real_ages) if real_ages else None
  minor_present = youngest is not None and youngest <= max_minor_age
  return _record(
      video_id=video_id,
      detector_version=detector_version,
      modality=sql.MODALITY_THUMBNAIL,
      taxonomy=sql.TAXONOMY_AGE_MINOR_PRESENCE,
      label=sql.LABEL_MINOR_PRESENCE,
      score=1.0 if minor_present else 0.0,
      evidence={
          'model': model,
          'min_real_human_age': youngest,
          'max_minor_age_threshold': max_minor_age,
          'real_human_count': real_human_count,
          'fictional_or_avatar_count': (
              len(normalized_persons) - real_human_count
          ),
          'verified_thumbnail_count': verified_thumbnail_count,
          'persons': normalized_persons,
          'reasoning': reasoning,
      },
  )


def parse_batch_output_row(
    row: Mapping[str, Any],
    model: str,
) -> ParsedVideoEvaluation:
  """Parses one row of a batch output table into `DetectionRecord`s."""
  # Everything needed is read from the row itself (`request` and
  # `max_minor_age`). A row is failed rather than guessed at when its answer is
  # missing anything that was asked for, because a guessed 0.0 would look like
  # a real "safe" result and the video would never be sent again.
  video_id = str(row.get('video_id') or '').strip()
  detector_version = str(row.get('detector_version') or '').strip() or 'unknown'

  def failed(message: str) -> ParsedVideoEvaluation:
    return ParsedVideoEvaluation(video_id, detector_version, False, message, ())

  if not video_id:
    return failed('The output row has no video_id.')
  if status_val := str(row.get('status') or '').strip():
    return failed(f'Batch row error status: {status_val}')
  requested = requested_evaluations(row.get('request'))
  if requested is None:
    return failed(
        'The stored request does not show which evaluations were requested.'
    )

  json_text = _extract_response_json_text(row.get('response'))
  if not json_text:
    return failed('Missing or empty candidate text in batch response')
  try:
    answer = json.loads(json_text)
  except json.JSONDecodeError as exc:
    return failed(f'Invalid JSON in candidate response: {exc}')
  if not isinstance(answer, Mapping):
    return failed('Candidate JSON root must be an object')

  persons = answer.get('persons')
  if requested.age_check and not isinstance(persons, list):
    return failed("The answer has no 'persons' list.")

  categories_obj = answer.get('categories')
  category_answers = (
      categories_obj if isinstance(categories_obj, Mapping) else {}
  )
  scores: dict[str, float] = {}
  for name in requested.categories:
    score = _finite_number(_dig(category_answers, name, 'score'))
    if score is None:
      return failed(f"The answer has no numeric score for category '{name}'.")
    scores[name] = round(min(1.0, max(0.0, score)), 4)

  reasoning = str(answer.get('reasoning') or '').strip()
  verified_thumb_count = int(row.get('verified_thumbnail_count') or 0)

  detections = [
      _record(
          video_id=video_id,
          detector_version=detector_version,
          modality=sql.MODALITY_THUMBNAIL_AND_METADATA,
          taxonomy=sql.TAXONOMY_AUDIT_EVALUATED,
          label=sql.LABEL_EVALUATED,
          score=0.0,
          evidence={
              'status': 'ok',
              'model': model,
              'verified_thumbnail_count': verified_thumb_count,
              'reasoning': reasoning,
          },
      )
  ]

  if requested.age_check:
    max_minor_age = row.get('max_minor_age')
    detections.append(
        _minor_presence_record(
            video_id=video_id,
            detector_version=detector_version,
            persons=persons,
            max_minor_age=(
                _LEGACY_MAX_MINOR_AGE
                if max_minor_age is None
                else int(max_minor_age)
            ),
            model=model,
            verified_thumbnail_count=verified_thumb_count,
            reasoning=reasoning,
        )
    )

  for name, score in scores.items():
    detections.append(
        _record(
            video_id=video_id,
            detector_version=detector_version,
            modality=sql.MODALITY_THUMBNAIL_AND_METADATA,
            taxonomy=f'{sql.TAXONOMY_CONTENT_PREFIX}{name}',
            label=name,
            score=score,
            evidence={
                'model': model,
                'category': name,
                'score': score,
                'verified_thumbnail_count': verified_thumb_count,
                'reasoning': reasoning,
            },
        )
    )

  return ParsedVideoEvaluation(
      video_id, detector_version, True, None, tuple(detections)
  )


def build_failure_audit_record(
    evaluation: ParsedVideoEvaluation, model: str
) -> DetectionRecord:
  """Marks a video whose answer could not be used as evaluated, with the error."""
  # Uses the same key as a successful `audit.evaluated` row so a later
  # re-evaluation overwrites it. The MERGE increments `failed_attempts` on
  # consecutive failures so transient remote fetch/RPC errors are retried up to
  # `sql.MAX_FAILED_ATTEMPTS` times before the video is held back.
  return _record(
      video_id=evaluation.video_id,
      detector_version=evaluation.detector_version,
      modality=sql.MODALITY_THUMBNAIL_AND_METADATA,
      taxonomy=sql.TAXONOMY_AUDIT_EVALUATED,
      label=sql.LABEL_EVALUATED,
      score=0.0,
      evidence={
          'status': 'failed',
          'failed_attempts': 1,
          'model': model,
          'error': evaluation.error_message,
      },
  )
