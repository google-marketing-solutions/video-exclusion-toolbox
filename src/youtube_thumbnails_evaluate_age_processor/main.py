"""Cloud Function for evaluating videos using Gemini."""

import base64
import datetime
import json
import logging
import os
import sys
from typing import Any

import functions_framework
from google import genai
from google.cloud import bigquery
from google.cloud import logging as cloud_logging
import jsonschema
import pydantic
import requests

# Fetched from variables available by default.
CLOUD_FUNCTION_NAME = os.environ.get(
    'K_SERVICE', 'failed_to_get_cloud_function_name'
)

# Set up logging either for local testing or for Cloud Run
LOG_LEVEL = logging.INFO
if os.getenv('IS_LOCAL_TEST', 'False') == 'True':
  logging.basicConfig(level=LOG_LEVEL, stream=sys.stdout)
else:
  logging_client = cloud_logging.Client()
  logging_client.setup_logging(log_level=LOG_LEVEL)

# Suppress the default logging from submodules.
logging.getLogger('google_genai').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

logger = logging.getLogger(CLOUD_FUNCTION_NAME)

GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
BQ_TARGET_DATASET = os.environ.get('VET_BIGQUERY_TARGET_DATASET')
BQ_TARGET_TABLE = os.environ.get('VET_BIGQUERY_TARGET_TABLE')
GEMINI_LOCATION = os.environ.get('VET_GEMINI_LOCATION')
GEMINI_MODEL = os.environ.get('VET_GEMINI_MODEL')

# The schema of the JSON in the event payload.
message_schema = {
    'type': 'object',
    'properties': {
        'system_instruction': {'type': 'string'},
        'prompt': {'type': 'string'},
        'batch_id': {'type': 'string'},
        'batch_part': {'type': 'string'},
        'total_batch_parts': {'type': 'string'},
        'videos': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'video_id': {'type': 'string'},
                },
                'required': [
                    'video_id',
                ],
            }
        },
    },
    'required': [
        'system_instruction',
        'prompt',
        'batch_id',
        'batch_part',
        'total_batch_parts',
        'videos',
    ],
}

SAFETY_SETTINGS = [
    genai.types.SafetySetting(
        category='HARM_CATEGORY_DANGEROUS_CONTENT',
        threshold='BLOCK_NONE',
    ),
]


class AgeEvaluation(pydantic.BaseModel):
  evaluated_description: str
  evaluated_age: int


class ResponseSchema(pydantic.BaseModel):
  items: list[AgeEvaluation]


def _write_data_to_bq(
    client: bigquery.Client,
    data: list[dict[str, Any]],
    dataset: str, table_name: str
) -> None:
  """Writes the given JSON data to BigQuery.

  Args:
      client: The BigQuery client.
      data: The data to write to BigQuery.
      dataset: The name of the BigQuery dataset.
      table_name: The name of the BigQuery table.
  """
  bq_destination = '.'.join([GOOGLE_CLOUD_PROJECT, dataset, table_name])

  if not data or data is None:
    logger.info('Nothing to write to BQ.')
    return

  logger.debug(
      'Writing %d records to BQ: %s: %s',
      len(data),
      bq_destination,
      json.dumps(data, indent=2),
  )

  errors = client.insert_rows_json(bq_destination, data)
  if not errors:
    logger.info(
        '%d records written to BQ: %s.',
        len(data),
        bq_destination,
    )
  else:
    logger.error(
        'Encountered errors while inserting rows to BQ: %s - %s',
        bq_destination,
        errors,
    )


def _get_thumbnail_urls(video_id: str) -> list[str] | None:
  """Fetches available YouTube thumbnails, getting the best quality per image type.

  Args:
    video_id: The YouTube video ID.

  Returns:
    Best available quality thumbnail URLs for the video, or None if no
    thumbnails were found.
  """
  prefixes = ['maxres', 'sd', 'hq', 'mq', '']  # Highest to lowest quality
  suffixes = ['default', '1', '2', '3']  # Different thumbnail identifiers
  base_url = f'https://i.ytimg.com/vi/{video_id}'
  thumbnail_urls = []

  for suffix in suffixes:
    for prefix in prefixes:
      thumbnail_url = f'{base_url}/{prefix}{suffix}.jpg'
      try:
        response = requests.head(thumbnail_url, timeout=5)
        if response.status_code == 200 and response.headers.get(
            'content-type', ''
        ).startswith('image/'):
          logger.debug('Found available thumbnail: %s', thumbnail_url)
          thumbnail_urls.append(thumbnail_url)
          break
        elif response.status_code != 404:
          logger.warning(
              'Checked %s: Status %s', thumbnail_url, response.status_code
          )

      except requests.exceptions.Timeout:
        logger.warning('Timeout looking up %s', thumbnail_url)
      except requests.exceptions.RequestException as e:
        logger.error('Error looking up %s: %s', thumbnail_url, e)

  if not thumbnail_urls or thumbnail_urls is None:
    logger.warning('No usable thumbnails found for video %s', video_id)
    return None

  return thumbnail_urls


def _evaluate_thumbnail_age(
    thumbnail_url: str,
    system_instruction: str,
    prompt: str,
    client: genai.Client
) -> dict[str, Any] | None:
  """Evaluate a single video using Gemini.

  Args:
      thumbnail_url (str): The URL of the thumbnail to evaluate.
      system_instruction (str): The system instruction to use for evaluation.
      prompt (str): The prompt to use for evaluation.
      client (genai.Client): The Gemini client to use for evaluation.

  Returns:
      A dictionary containing the evaluation results, or None if the evaluation
      failed.
  """
  logger.debug('Prompt:\n%s', prompt)
  logger.debug('System instruction:\n%s', system_instruction)

  try:
    image_part = genai.types.Part.from_uri(
        file_uri=thumbnail_url, mime_type='image/jpeg'
    )
    model_response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[image_part, prompt],
        config=genai.types.GenerateContentConfig(
            system_instruction=system_instruction,
            safety_settings=SAFETY_SETTINGS,
            response_mime_type='application/json',
            response_schema=ResponseSchema,
        )
    )
  except genai.errors.APIError as e:
    logger.warning(
        '%s: Failed to evaluate content for video %s: %s',
        CLOUD_FUNCTION_NAME,
        thumbnail_url,
        e,
    )
    return {
        'datetime_updated': (
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ),
        'description': (
            f'Failed to evaluate thumbnail at {thumbnail_url}: \n{e}.'
        ),
    }

  try:
    response = json.loads(model_response.text)
  except json.decoder.JSONDecodeError as e:
    logger.warning(
        'Unable to parse response as JSON. Response: %s \n Error: %s',
        model_response.text,
        e,
    )
    return {
        'datetime_updated': (
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ),
        'description': (
            f'Unable to parse response as JSON for thumbnail {thumbnail_url}:'
            f' \n{e}.'
        ),
    }

  response['datetime_updated'] = datetime.datetime.now().strftime(
      '%Y-%m-%d %H:%M:%S'
  )
  logger.debug('Response: %s', response)

  return response


def run(
    system_instruction: str,
    prompt: str,
    batch_id: str,
    videos: list[dict[str, str]],
    batch_part: str,
    total_batch_parts: str,
) -> None:
  """Orchestration to evaluate age of persons in thumbnails.

  Args:
      system_instruction: The system instruction to use for evaluation.
      prompt: The prompt to use for evaluation.
      batch_id: The batch ID to process.
      videos: The videos to process.
      batch_part: The current batch part being processed.
      total_batch_parts: The total number of batch parts.
  """

  logger.debug(
      'Batch ID: %s (%s of %s): Video IDs: %s',
      batch_id,
      batch_part,
      total_batch_parts,
      json.dumps(videos, indent=2),
  )
  logger.info(
      'Batch ID: %s (%s of %s): Videos - %s',
      batch_id,
      batch_part,
      total_batch_parts,
      len(videos),
  )

  bq_client = bigquery.Client()
  genai_client = genai.Client(
      project=GOOGLE_CLOUD_PROJECT, location=GEMINI_LOCATION, vertexai='True'
  )

  for video in videos:
    thumbnail_urls = _get_thumbnail_urls(video['video_id'])
    if thumbnail_urls is None:
      _write_data_to_bq(
          client=bq_client,
          data=[{
              'video_id': video['video_id'],
              'datetime_updated': (
                  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
              ),
              'evaluation_model_id': 'NONE',
              'evaluated_description': (
                  f'No usable thumbnails found for video {video["video_id"]}.'
              ),
          }],
          dataset=BQ_TARGET_DATASET,
          table_name=BQ_TARGET_TABLE,
      )
      continue

    for thumbnail_url in thumbnail_urls:
      response = _evaluate_thumbnail_age(
          thumbnail_url=thumbnail_url,
          system_instruction=system_instruction,
          prompt=prompt,
          client=genai_client
      )

      logger.debug('Response: %s', response)
      if response is None:
        logger.info(
            'Unable to age in thumbnail %s, skipping.',
            thumbnail_url,
        )
        continue

      response_enriched = [
          {
              'video_id': video['video_id'],
              'thumbnail_url': thumbnail_url,
              'datetime_updated': response['datetime_updated'],
              'evaluation_model_id': GEMINI_MODEL,
              'evaluated_description': item['evaluated_description'],
              'evaluated_age': item['evaluated_age'],
          }
          for item in response['items']
      ]

      logger.debug(
          'Response enriched: %s', json.dumps(response_enriched, indent=2)
      )

      _write_data_to_bq(
          client=bq_client,
          data=response_enriched,
          dataset=BQ_TARGET_DATASET,
          table_name=BQ_TARGET_TABLE,
      )

  logger.info('All videos processed.')


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def main(cloud_event: functions_framework.cloud_event) -> None:
  """The entry point: extract the data from the payload and starts the job.

  The pub/sub message must match the message_schema object above.

  Args:
      cloud_event: A dictionary representing the event data payload.

  Raises:
      jsonschema.exceptions.ValidationError if the message from pub/sub is not
      what is expected.
  """
  logger.info('Thumbnail age evaluation processor started.')
  logger.debug('cloud_event: %s', cloud_event)
  data = base64.b64decode(cloud_event.data['message']['data']).decode('UTF-8')
  logger.info('Decoded message: %s', data)
  message_json = json.loads(data)
  logger.debug('JSON message: %s', data)

  jsonschema.validate(instance=message_json, schema=message_schema)

  run(
      system_instruction=message_json.get('system_instruction'),
      prompt=message_json.get('prompt'),
      batch_id=message_json.get('batch_id'),
      videos=message_json.get('videos'),
      batch_part=message_json.get('batch_part'),
      total_batch_parts=message_json.get('total_batch_parts'),
  )

  logger.info('Thumbnail age evaluation processor finished.')
