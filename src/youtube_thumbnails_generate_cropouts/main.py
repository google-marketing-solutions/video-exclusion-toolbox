# Copyright 2023 Google LLC
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

"""Detect objects in a thumbnail and store the details in BQ."""

import base64
import datetime
import io
import json
import logging
import os
import sys
from typing import Any
import uuid

import google.auth
import google.auth.credentials
from google.cloud import bigquery
from google.cloud import storage
import jsonschema
import pandas as pd
import PIL.Image
import requests

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BIGQUERY_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The bucket to write the thumbnails to.
THUMBNAIL_CROP_BUCKET = os.environ.get('VID_EXCL_THUMBNAIL_CROP_BUCKET')

# Selector of what labels of objects will be cropped out.
# For age recognition, 'Face' and 'Person' are recommended.
OBJECTS_TO_CROP_AND_STORE = ['Face', 'Person']
# BQ Table name to store the detected object's metadata. This is not expexted
# to be configurable and so is not exposed as an environmental variable
BQ_SOURCE_TABLE_NAME = 'YouTubeThumbnailsWithAnnotations'
BQ_TARGET_TABLE_NAME = 'YouTubeThumbnailCropouts'
CHARS_TO_REPLACE_IN_IMAGE_NAME = [':', '/', '.', '?', '#', '&', '=', '+']

# The schema of the JSON in the event payload.
MESSAGE_SCHEMA = {
    'type': 'object',
    'properties': {
        'video_id': {'type': 'string'},
    },
    'required': [
        'video_id',
    ],
}


def main(event: dict[str, Any], context: dict[str, Any]) -> None:
  """The entry point: extract the data from the payload and starts the job.

  The pub/sub message must match the definition in main.MESSAGE_SCHEMA.

  Args:
      event: A dictionary representing the event data payload.
      context: An object containing metadata about the event.

  Raises:
      ValidationError: The message from pub/sub is not what is expected.
  """
  # deleting context, as it's not required, but gets passed in along the "event"
  # data when the Cloud Function is triggered from Pub/Sub
  del context
  logger.info('Thumbnail object cropping service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=MESSAGE_SCHEMA)

  video_id = message_json.get('video_id')

  run(video_id=video_id)

  logger.info(
      'Thumbnail object cropping for video %s processing done.', video_id
  )


def run(video_id: str) -> None:
  """Crops objects for all thumbnails in a YouTube video.

  Args:
      video_id: The ID of the video to crop the objects thumbnails for.
  """
  credentials = _get_auth_credentials()

  logger.info('Starting to process video %s thumbnails.', video_id)
  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client(
      project=GOOGLE_CLOUD_PROJECT, credentials=credentials
  )
  query = f"""
    SELECT video_id
    FROM {BIGQUERY_DATASET}.{BQ_TARGET_TABLE_NAME}
    WHERE video_id = '{video_id}'
    """

  rows = client.query(query).result()
  if rows.total_rows > 0:
    logger.info('Thumbnails for video %s already processed.', video_id)
  else:
    _crop_objects_from_video_thubmnails(video_id, client=client)
    logger.info('Finished processing thumbnails for video %s.', video_id)


def _crop_objects_from_video_thubmnails(
    video_id: str, client: bigquery.Client
) -> None:
  """Orchestrates cropping objects, storing them in GCS, keeping records in BQ.

  Args:
      video_id: The YouTube Video ID to process.
      client: BigQuery client to use for BigQuery queries.
  """

  labels_for_query = ["'" + label + "'" for label in OBJECTS_TO_CROP_AND_STORE]
  labels_for_query = '(' + ','.join(labels_for_query) + ')'

  query = f"""
    SELECT *
    FROM {BIGQUERY_DATASET}.{BQ_SOURCE_TABLE_NAME}
    WHERE video_id = '{video_id}'
    AND label IN {labels_for_query}
    """

  source_data = client.query(query).to_dataframe()

  if not source_data.empty:
    logger.info(
        'Cropping %d objects for video_id %s.',
        len(source_data.index),
        video_id,
    )

    cropouts = _generate_cropouts_from_dataframe(source_data)

    client = storage.Client()
    for cropout in cropouts:
      _save_image_to_gcs(
          client=client,
          image=cropout['image_object'],
          image_name=cropout['file_name'],
          bucket_name=THUMBNAIL_CROP_BUCKET,
          prefix=cropout['video_id'],
      )
    logger.info(
        '%d object(s) stored in GCS.',
        len(cropouts),
    )

    cropouts = pd.DataFrame(cropouts).drop('image_object', axis=1)

    _write_results_to_bq(
        cropouts, BQ_TARGET_TABLE_NAME
    )
  else:
    logger.error(
        'There is no record of video %s in table %s.',
        video_id,
        BQ_SOURCE_TABLE_NAME,
    )


def _generate_cropouts_from_dataframe(
    cropout_data: pd.DataFrame
) -> list[dict[PIL.Image.Image, dict[str, datetime]] | None]:
  """Generates cropouts from an image and preserve provided metadata.

  Args:
    cropout_data: A dataframe with coordinates of each cropout.

  Returns:
    Dictionaries with cropped image objects and their metadata.
    Returns an empty list if the cropout_data input is an empty dataframe.

    Example:
    [
      {
        'image_object': <PIL.Image.Image object>,
        'video_id': 'video_id_1',
        'thumbnail_url': 'thumbnail_url_1',
        'label': 'Face',
        'confidence': '0.996'
        'file_name': 'generated_file_name_1',
        'gs_file_path': 'gs://bucket/video_id_1/generated_file_name_1',
        'fuse_file_path': '/gcs/bucket/video_id_1/generated_file_name_2',
        'datetime_updated': datetime.datetime(2023, 9, 28, 7, 55, 54)
      },
      {
        'image_object': <PIL.Image.Image object>,
        'video_id': 'video_id_2',
        'thumbnail_url': 'thumbnail_url_2',
        'label': 'Person',
        'confidence': '0.984'
        'file_name': 'generated_file_name_2',
        'gs_file_path': 'gs://bucket/video_id_2/generated_file_name_2',
        'fuse_file_path': '/gcs/bucket/video_id_2/generated_file_name_2',
        'datetime_updated': datetime.datetime(2023, 9, 28, 7, 55, 54)
      }
    ]
  """
  if cropout_data.empty:
    logger.info('Nothing to crop.')
    return []

  cropout_data.sort_values(by='thumbnail_url', inplace=True)

  cropped_images = []
  url = cropout_data['thumbnail_url'].iloc[0]
  thumbnail = _get_image_from_url(url)

  for _, row in cropout_data.iterrows():

    if url != row['thumbnail_url']:
      url = row['thumbnail_url']
      thumbnail = _get_image_from_url(row['thumbnail_url'])

    if not thumbnail:
      logger.error('Unable to get thumbnail from url %s.', url)
      continue

    image_object = _cropout_from_image(
        image=thumbnail,
        top_left_x=float(row['top_left_x']),
        top_left_y=float(row['top_left_y']),
        bottom_right_x=float(row['bottom_right_x']),
        bottom_right_y=float(row['bottom_right_y']),
    )
    file_name = _generate_thumbnail_name(
        video_url=row['thumbnail_url'],
        label=row['label'],
    )
    gs_file_path = '/'.join(
        ['gs:/', THUMBNAIL_CROP_BUCKET, row['video_id'], file_name]
    )
    fuse_file_path = '/'.join(
        ['/gcs', THUMBNAIL_CROP_BUCKET, row['video_id'], file_name]
    )

    cropped_image = {
        'image_object': image_object,
        'video_id': row['video_id'],
        'thumbnail_url': row['thumbnail_url'],
        'label': row['label'],
        'confidence': row['confidence'],
        'file_name': file_name,
        'gs_file_path': gs_file_path,
        'fuse_file_path': fuse_file_path,
        'datetime_updated': datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        ),
    }

    cropped_images.append(cropped_image)

  logger.info(
      'Cropped %d object(s) from thumbnail %s.',
      len(cropped_images),
      url,
  )

  return cropped_images


def _generate_thumbnail_name(video_url: str, label: str) -> str:
  """Generates a sanitized file name for a GCS blob.

  Args:
    video_url: YouTube Video url.
    label: Object label.

  Returns:
    New object name for GCS in format:
    {label}-{random-UUID}-{sanitized video_id}.
  """
  sanitized_url = video_url
  for ch in CHARS_TO_REPLACE_IN_IMAGE_NAME:
    sanitized_url = sanitized_url.replace(ch, '_')
  # The "_" is for conciseness and readability purposes only, there's no
  # technical requirement, and so a simplictic replacement is sufficient.
  sanitized_url = sanitized_url.replace('___', '_').replace('__', '_')
  return f'{label}-{str(uuid.uuid4())[-6:]}-{sanitized_url}'


def _get_image_from_url(url: str) -> PIL.Image.Image | None:
  """Gets an image from a url.

  Args:
    url: The url to get the image from.

  Returns:
    Image: The image object.
    Returns None if the url is not found.
  """
  response = requests.get(url, stream=True)
  if response.status_code != 200:
    logger.error('Unable to get thumbnail from url %s', url)
    return None
  return PIL.Image.open(response.raw)


def _save_image_to_gcs(
    client: storage.Client,
    image: PIL.Image,
    image_name: str,
    bucket_name: str,
    prefix: str = '',
) -> None:
  """Uploads an image object to a GCS bucket.

  Args:
    client: An initialized GCS client.
    image: Image object.
    image_name: Name of the object to be created as.
    bucket_name: Name oth the GCS bucket for the object to be created in.
    prefix: the prefix of the file, acts as a folder in GCS
  """
  full_path = '/'.join([prefix, image_name])
  bucket = client.bucket(bucket_name)
  img_byte_array = io.BytesIO()
  image.save(img_byte_array, format='JPEG')
  image_blob = bucket.blob(full_path)
  image_blob.upload_from_string(
      img_byte_array.getvalue(), content_type='image/jpeg'
  )
  logger.debug('Saved image %s to %s', full_path, bucket_name)


def _cropout_from_image(
    image: PIL.Image.Image,
    top_left_x: float,
    top_left_y: float,
    bottom_right_x: float,
    bottom_right_y: float,
) -> PIL.Image.Image:
  """Crop an image based on Top Left and Bottom Right coordinates.

  Accepts relative and absolute coordinates.
  If all coordinates are <1, the function assumes these are relative coordinates
  and converts them to absolute values.
  If the Bottom Right coordinates are 0, the function returns the full image.

  Args:
      image: The original image to crop a cutout from.
      top_left_x: The x-coordinate of the top-left corner of the crop
        rectangle.
      top_left_y: The y-coordinate of the top-left corner of the crop
        rectangle.
      bottom_right_x: The x-coordinate of the bottom-right corner of the
        crop rectangle.
      bottom_right_y: The y-coordinate of the bottom-right corner of the
        crop rectangle.

  Returns:
      Image: The cropped image.
  """

  width = image.width
  height = image.height

  if bottom_right_x == 0 and bottom_right_y == 0:
    return image

  if (top_left_x <= 1 and top_left_y <= 1 and bottom_right_x <= 1 and
      bottom_right_y <= 1):
    top_left_x = top_left_x * width
    top_left_y = top_left_y * height
    bottom_right_x = bottom_right_x * width
    bottom_right_y = bottom_right_y * height

  return image.crop(
      (top_left_x, top_left_y, bottom_right_x, bottom_right_y)
  )


def _get_auth_credentials() -> google.auth.credentials.Credentials:
  """Return credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials


def _write_results_to_bq(data: pd.DataFrame, table_id: str) -> None:
  """Write the YouTube dataframe to BQ.

  Args:
      data: The dataframe based on the YouTube data.
      table_id: The id of the BQ table.
  """

  bq_destination = '.'.join(
      [GOOGLE_CLOUD_PROJECT, BIGQUERY_DATASET, table_id]
  )

  if data is None or data.empty:
    logger.info('Nothing to write to BQ.')
  else:
    logger.info('Writing results to BQ: %s', bq_destination)
    data_to_write = data.to_dict(orient='records')
    client = bigquery.Client()

    errors = client.insert_rows_json(bq_destination, data_to_write)
    if not errors:
      logger.info('%d records written to BQ.', len(data_to_write))
    else:
      logger.error('Encountered errors while inserting rows: %s', errors)
