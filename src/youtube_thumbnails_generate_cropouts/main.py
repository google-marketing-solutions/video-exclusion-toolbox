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

# BQ Table name to store the detected object's metadata. This is not expexted
# to be configurable and so is not exposed as an environmental variable
BQ_TARGET_TABLE_NAME = 'YouTubeThumbnailCropouts'
CHARS_TO_REPLACE_IN_IMAGE_NAME = [':', '/', '.', '?', '#', '&', '=', '+']

# The schema of the JSON in the event payload.
MESSAGE_SCHEMA = {
    'type': 'object',
    'properties': {
        'video_id': {'type': 'string'},
        'objects': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'thumbnail_url': {'type': 'string'},
                    'label': {'type': 'string'},
                    'top_left_x': {'type': 'number'},
                    'top_left_y': {'type': 'number'},
                    'bottom_right_x': {'type': 'number'},
                    'bottom_right_y': {'type': 'number'},
                },
                'required': [
                    'thumbnail_url',
                    'label',
                    'top_left_x',
                    'top_left_y',
                    'bottom_right_x',
                    'bottom_right_y',
                ]
            },
        },
    },
    'required': [
        'video_id',
        'objects',
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
  message_json = json.loads(base64.b64decode(event['data']).decode('utf-8'))

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=MESSAGE_SCHEMA)

  video_id = message_json.get('video_id')
  objects = message_json.get('objects')

  run(video_id=video_id, objects=objects)

  logger.info(
      'Thumbnail object cropping for video %s processing done.', video_id
  )


def run(video_id: str, objects: list[dict[str | float]]) -> None:
  """Crops objects for all thumbnails in a YouTube video.

  Args:
      video_id: The ID of the video to crop the objects thumbnails for.
      objects: The objects to crop.
  """

  logger.info('Starting to process video %s thumbnails.', video_id)

  if not objects:
    logger.error(
        'No objects were received to be processed for video %s.',
        video_id,
    )
    return

  logger.info('Expecting %d objects for video_id %s.', len(objects), video_id)

  # transpose the list of object dictionaries into a dictionary of thumbnails
  thumbnail_data = {}
  for item in objects:
    if item['thumbnail_url'] not in thumbnail_data:
      thumbnail_data[item['thumbnail_url']] = []
    thumbnail_data[item['thumbnail_url']].append(item)

  cropouts = []
  for thumbnail_url in thumbnail_data:
    cropouts.extend(
        _generate_cropouts(thumbnail_url, thumbnail_data[thumbnail_url])
    )

  if not cropouts:
    logger.error('Nothing was cropped for video %s.', video_id)
    return

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


def _generate_cropouts(
    thumbnail_url: str,
    cropout_data: list[dict[str, Any]]
) -> list[dict[PIL.Image.Image, dict[str, datetime]]]:
  """Generates cropouts from an image and preserve provided metadata.

  Args:
    thumbnail_url: The url of the thumbnail to crop.
    cropout_data: Dictionaries with coordinates of each cropout.

  Returns:
    Dictionaries with cropped image objects and their metadata.
    Returns an empty list if the data input is an empty list.

    Example:
    [
      {
        'image_object': <PIL.Image.Image object>,
        'video_id': 'video_id',
        'thumbnail_url': 'thumbnail_url',
        'label': 'Face',
        'confidence': '0.996'
        'file_name': 'generated_file_name_1',
        'gs_file_path': 'gs://bucket/video_id_1/generated_file_name_1',
        'fuse_file_path': '/gcs/bucket/video_id_1/generated_file_name_1',
        'datetime_updated': datetime.datetime(2023, 9, 28, 7, 55, 54)
      },
      {
        'image_object': <PIL.Image.Image object>,
        'video_id': 'video_id',
        'thumbnail_url': 'thumbnail_url',
        'label': 'Person',
        'confidence': '0.984'
        'file_name': 'generated_file_name_2',
        'gs_file_path': 'gs://bucket/video_id_2/generated_file_name_2',
        'fuse_file_path': '/gcs/bucket/video_id_2/generated_file_name_2',
        'datetime_updated': datetime.datetime(2023, 9, 28, 7, 55, 54)
      }
    ]
  """
  cropped_images = []

  if not cropout_data:
    return cropped_images

  thumbnail = _get_image_from_url(thumbnail_url)
  if not thumbnail:
    logger.warning('Unable to get thumbnail from url %s.', thumbnail_url)
    return cropped_images

  for record in cropout_data:
    image_object = _cropout_from_image(
        image=thumbnail,
        top_left_x=float(record['top_left_x']),
        top_left_y=float(record['top_left_y']),
        bottom_right_x=float(record['bottom_right_x']),
        bottom_right_y=float(record['bottom_right_y']),
    )
    file_name = _generate_thumbnail_name(
        video_url=record['thumbnail_url'],
        label=record['label'],
    )
    gs_file_path = '/'.join(
        ['gs:/', THUMBNAIL_CROP_BUCKET, record['video_id'], file_name]
    )
    fuse_file_path = '/'.join(
        ['/gcs', THUMBNAIL_CROP_BUCKET, record['video_id'], file_name]
    )

    cropped_image = {
        'image_object': image_object,
        'video_id': record['video_id'],
        'thumbnail_url': record['thumbnail_url'],
        'label': record['label'],
        'confidence': record['confidence'],
        'file_name': file_name,
        'gs_file_path': gs_file_path,
        'fuse_file_path': fuse_file_path,
        'datetime_updated': datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S'
        ),
    }

    cropped_images.append(cropped_image)

  logger.info(
      'Cropped %d of %d object(s) from thumbnail %s.',
      len(cropped_images),
      len(cropout_data),
      thumbnail_url,
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
    data_to_write = data.to_dict(orient='records')
    client = bigquery.Client()

    errors = client.insert_rows_json(bq_destination, data_to_write)
    if not errors:
      logger.info(
          '%d records written to BQ: %s.', len(data_to_write), bq_destination
      )
    else:
      logger.error('Encountered errors while inserting rows: %s', errors)
