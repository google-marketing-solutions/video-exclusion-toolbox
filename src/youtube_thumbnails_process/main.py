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
from google.cloud import vision
import jsonschema
import pandas as pd
import PIL.Image
import requests

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Selector of what labels of objects will be cropped out.
# For age recognition, 'Face' and 'Parson' are recommended.
OBJECTS_TO_CROP_AND_STORE = ['Face', 'Person']
# BQ Table name to store the detected object's metadata. This is not expexted
# to be configurable and so is not exposed as an environmental variable
BQ_TABLE_NAME = 'YouTubeThumbnailsWithAnnotations'

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# Switch whether to store cropped thumbnail objecs in GCS.
CROP_AND_STORE_OBJECTS = os.environ.get(
    'VID_EXCL_CROP_AND_STORE_OBJECTS', 'False'
).lower() in ('true', '1', 't')
# The bucket to write the thumbnails to.
THUMBNAIL_CROP_BUCKET = os.environ.get('VID_EXCL_THUMBNAIL_CROP_BUCKET')

THUMBNAIL_URL = 'https://i.ytimg.com/vi/{video_id}/{thumbnail_name}.jpg'
CHARS_TO_REPLACE_IN_IMAGE_NAME = [':', '/', '.', '?', '#', '&', '=', '+']

THUMBNAIL_SET_DEFAULT = [
    'maxresdefault',
    'hq720',
    'sddefault',
    'hqdefault',
    '0',
    'mqdefault',
    'default',
]
THUMBNAIL_SET_1 = ['sd1', 'hq1', 'mq1', '1']
THUMBNAIL_SET_2 = ['sd2', 'hq2', 'mq2', '2']
THUMBNAIL_SET_3 = ['sd3', 'hq3', 'mq3', '3']

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
  logger.info('Thumbnail processor service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=MESSAGE_SCHEMA)

  video_id = message_json.get('video_id')

  run(video_id=video_id)

  logger.info('Video %s processing done.', video_id)


def run(video_id: str) -> None:
  """Orchestrates processing all videos in a csv file.

  Args:
      video_id: The ID of the video to process the thumbnails for.
  """
  credentials = _get_auth_credentials()

  logger.info('Starting to process video %s thumbnails.', video_id)
  logger.info('Connecting to: %s BigQuery.', GOOGLE_CLOUD_PROJECT)
  client = bigquery.Client(
      project=GOOGLE_CLOUD_PROJECT, credentials=credentials
  )
  query = f"""
    SELECT video_id
    FROM {BQ_DATASET}.{BQ_TABLE_NAME}
    WHERE video_id = '{video_id}'
    """
  rows = client.query(query).result()
  if rows.total_rows > 0:
    logger.info('Thumbnails for video %s already processed.', video_id)
  else:
    _process_video(video_id)
    logger.info('Processed thumbnails for video %s.', video_id)


def _process_video(video_id: str) -> None:
  """Orchestrates pulling YouTube data and output it to BigQuery.

  Args:
      video_id: The YouTube Video ID to process.
  """
  logger.info('Looking up thumbnails for video %s', video_id)
  thumbnails = (
      _get_best_resolution_thumbnail(THUMBNAIL_SET_DEFAULT, video_id)
      | _get_best_resolution_thumbnail(THUMBNAIL_SET_1, video_id)
      | _get_best_resolution_thumbnail(THUMBNAIL_SET_2, video_id)
      | _get_best_resolution_thumbnail(THUMBNAIL_SET_3, video_id)
  )

  extracted_objects = []
  for url in thumbnails.keys():
    thumbnail_data = _extract_objects_df_from_thumbnail(
        thumbnail_image=thumbnails[url]
    )
    thumbnail_data.insert(0, 'thumbnail_url', url)
    thumbnail_data.insert(0, 'video_id', video_id)
    extracted_objects.append(thumbnail_data)

  all_extracted_objects = pd.concat(extracted_objects, ignore_index=True)

  logger.info(
      'Extracted %d object(s) from %d thumbnail(s).',
      len(all_extracted_objects.index),
      len(thumbnails),
  )

  _write_results_to_bq(
      all_extracted_objects, BQ_TABLE_NAME
  )

  if CROP_AND_STORE_OBJECTS:
    filtered_objects = all_extracted_objects[
        all_extracted_objects['label'].isin(OBJECTS_TO_CROP_AND_STORE)
    ]

    logger.info(
        'Filtered %d object(s) to be cropped from thumbanils and stored in'
        ' GCS.',
        len(filtered_objects.index),
    )

    cropouts = {}
    for thumbnail_url in thumbnails.keys():
      cropouts = cropouts | _generate_cropouts_from_image(
          thumbnails[thumbnail_url],
          filtered_objects[filtered_objects['thumbnail_url'] == thumbnail_url],
      )

    client = storage.Client()
    for image_name in cropouts.keys():
      _save_image_to_gcs(
          client=client,
          image=cropouts[image_name],
          image_name=image_name,
          bucket_name=THUMBNAIL_CROP_BUCKET,
      )


def _generate_cropouts_from_image(
    thumbnail: PIL.Image.Image, cropout_data: pd.DataFrame
) -> dict[PIL.Image.Image | None]:
  """Generates cropouts from an image.

  Args:
    thumbnail: An image from which to crop out images.
    cropout_data: A dataframe with coordinates of each cropout.

  Returns:
    A dictionary with image name as keys, cropped image objects as values.
    Returns an empty dictionary if the cropout_data input is an empty dataframe.
  """
  cropped_images = {}

  if not cropout_data.empty:
    for _, row in cropout_data.iterrows():
      cropped_image = _cropout_from_image(
          image=thumbnail,
          top_left_x=row['top_left_x'],
          top_left_y=row['top_left_y'],
          bottom_right_x=row['bottom_right_x'],
          bottom_right_y=row['bottom_right_y'],
      )
      image_name = _generate_thumbnail_name(
          video_id=row['video_id'],
          video_url=row['thumbnail_url'],
          label=row['label'],
      )
      cropped_images[image_name] = cropped_image

    logger.info(
        'Generated %d crop-out(s) from thumbnail %s.',
        len(cropped_images),
        cropout_data['thumbnail_url'].iloc[0],
    )
  else:
    logger.info('Nothing to crop.')

  return cropped_images


def _extract_objects_df_from_thumbnail(
    thumbnail_image: PIL.Image.Image,
) -> pd.DataFrame:
  """Orchestrates the full processing pipeline for a single thumbnail.

  Args:
    thumbnail_image: The image object of the thumbnail.

  Returns:
    A dataframe of all the objects detected in the thumbnail.
  """
  objects = _localized_object_annotations_from_image(thumbnail_image)
  objects.extend(_face_annotations_from_image(thumbnail_image))

  return pd.DataFrame(objects)


def _generate_thumbnail_name(video_id: str, video_url: str, label: str) -> str:
  """Generate a sanitized file name for a GCS blob.

  Args:
    video_id: YouTube video ID, acts as a GCP folder.
    video_url: YouTube Video url.
    label: Object label.

  Returns:
    New object name for GCS in format:
    {video_id}/{label}-{random-UUID}-{sanitized video_id}.
  """
  sanitized_url = video_url
  for ch in CHARS_TO_REPLACE_IN_IMAGE_NAME:
    sanitized_url = sanitized_url.replace(ch, '_')
  # The "_" is for conciseness and readability purposes only, there's no
  # technical requirement, and so a simplictic replacement is sufficient.
  sanitized_url = sanitized_url.replace('___', '_').replace('__', '_')
  return f'{video_id}/{label}-{str(uuid.uuid4())[-6:]}-{sanitized_url}.png'


def _save_image_to_gcs(
    client: storage.Client, image: PIL.Image, image_name: str, bucket_name: str
) -> None:
  """Uploads an image object to a GCS bucket.

  Args:
    client: An initialized GCS client.
    image: Image object.
    image_name: Name of the object to be created as.
    bucket_name: Name oth the GCS bucket for the object to be created in.

  Returns:
    Url of the newly created GCS object.
  """
  bucket = client.bucket(bucket_name)
  img_byte_array = io.BytesIO()
  image.save(img_byte_array, format='JPEG')
  image_blob = bucket.blob(image_name)
  image_blob.upload_from_string(
      img_byte_array.getvalue(), content_type='image/jpeg'
  )
  logger.debug('Saved image %s to %s', image_name, bucket_name)


def _cropout_from_image(
    image: PIL.Image.Image,
    top_left_x: float,
    top_left_y: float,
    bottom_right_x: float,
    bottom_right_y: float,
) -> PIL.Image.Image:
  """Crops an image based on relative coordinates.

  Args:
      image: The original image to crop a cutout from.
      top_left_x: The x-coordinate of the top-left corner of the crop
        rectangle, as a percentage of the width of the image.
      top_left_y: The y-coordinate of the top-left corner of the crop
        rectangle, as a percentage of the height of the image.
      bottom_right_x: The x-coordinate of the bottom-right corner of the
        crop rectangle, as a percentage of the width of the image.
      bottom_right_y: The y-coordinate of the bottom-right corner of the
        crop rectangle, as a percentage of the height of the image.

  Returns:
      Image: The cropped image.
  """

  width = image.width
  height = image.height

  top_left_x = top_left_x * width
  top_left_y = top_left_y * height
  bottom_right_x = bottom_right_x * width
  bottom_right_y = bottom_right_y * height

  return image.crop(
      (top_left_x, top_left_y, bottom_right_x, bottom_right_y)
  )


def _get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials


def _localized_object_annotations_from_image(
    image: PIL.Image.Image,
) -> list[dict[str]]:
  """Gets labels from the Google Vision API for an image object.

  Args:
      image: The image object to retrieve localized object annotations for.

  Returns:
      list: A list of dictionaries containing localized annotations for each
        identified object.
  """
  logger.info('Getting localized object annotations.')

  client = vision.ImageAnnotatorClient()

  buffer = io.BytesIO()
  image.save(buffer, format='PNG')
  vision_image = vision.Image(content=buffer.getvalue())
  objects = client.object_localization(
      image=vision_image
  ).localized_object_annotations

  logger.info('Number of objects found: %s', len(objects))

  return [_parse_vision_object_annotations(object_) for object_ in objects]


def _face_annotations_from_image(image: PIL.Image.Image) -> list[dict[str]]:
  """Gets labels from the Google Vision API for an image object.

  Args:
      image: The image object to retrieve faces for.

  Returns:
      list: Dictionaries containing annotations for each.
        identified face.
  """
  logger.info('Getting faces.')

  client = vision.ImageAnnotatorClient()

  buffer = io.BytesIO()
  image.save(buffer, format='PNG')
  vision_image = vision.Image(content=buffer.getvalue())
  faces = client.face_detection(image=vision_image).face_annotations

  logger.info('Number of faces found: %s', len(faces))

  width, height = image.size
  parsed_annotations = [_parse_face_annotations(face) for face in faces]

  # Face annotation coordinates are in absolute dimensions, this has to be
  # converted to relative coordinates.
  for parsed_annotation in parsed_annotations:
    parsed_annotation['top_left_x'] = parsed_annotation['top_left_x'] / width
    parsed_annotation['top_left_y'] = parsed_annotation['top_left_y'] / height
    parsed_annotation['bottom_right_x'] = (
        parsed_annotation['bottom_right_x'] / width
    )
    parsed_annotation['bottom_right_y'] = (
        parsed_annotation['bottom_right_y'] / height
    )

  return parsed_annotations


def _parse_vision_object_annotations(
    vision_object: vision.LocalizedObjectAnnotation) -> dict[str]:
  """Gets labels from the Google Vision API for an image at the given URL.

  Args:
      vision_object: An object from the Vision API.
      (https://cloud.google.com/python/docs/reference/vision/latest/google.cloud.vision_v1.types.LocalizedObjectAnnotation)

  Returns:
      dict: Parsed object annotations.
  """
  return {
      'label': vision_object.name,
      'confidence': vision_object.score,
      'top_left_x': vision_object.bounding_poly.normalized_vertices[0].x,
      'top_left_y': vision_object.bounding_poly.normalized_vertices[0].y,
      'bottom_right_x': vision_object.bounding_poly.normalized_vertices[2].x,
      'bottom_right_y': vision_object.bounding_poly.normalized_vertices[2].y,
      'datetime_updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
  }


def _parse_face_annotations(vision_object: vision.FaceAnnotation) -> dict[str]:
  """Gets labels from the Google Vision API from a face annotation object.

  Args:
      vision_object: An object from the Vision API.
        (https://cloud.google.com/python/docs/reference/vision/latest/google.cloud.vision_v1.types.FaceAnnotation)

  Returns:
      dict: Parsed face annotations.
  """
  return {
      'label': 'Face',
      'confidence': vision_object.detection_confidence,
      'top_left_x': vision_object.bounding_poly.vertices[0].x,
      'top_left_y': vision_object.bounding_poly.vertices[0].y,
      'bottom_right_x': vision_object.bounding_poly.vertices[2].x,
      'bottom_right_y': vision_object.bounding_poly.vertices[2].y,
      'datetime_updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
  }


def _get_best_resolution_thumbnail(
    file_names: list[str], video_id: str
) -> dict[PIL.Image.Image|None]:
  """Writes the YouTube dataframe to BQ.

  Args:
      file_names: The an ordered list of thumbnail filenames to check.
      video_id: The id of YouTube video.

  Returns:
      A dictionary with the thumbnail's url as the key and an Image object as
      value. Returns an empty dictionary if no thumbanils were found.
  """
  logger.info('Getting the best resolution for %s', file_names)

  for file_name in file_names:
    url = THUMBNAIL_URL.format(video_id=video_id, thumbnail_name=file_name)

    response = requests.get(url, stream=True)
    if requests.get(url).status_code == 200:
      logger.info('Best resolution was found at %s', url)
      return {url: PIL.Image.open(response.raw)}
  logger.info('Did not find a usable thumbnail for video %s', video_id)
  return {}


def _write_results_to_bq(data: pd.DataFrame, table_id: str) -> None:
  """Write the YouTube dataframe to BQ.

  Args:
      data: The dataframe based on the YouTube data.
      table_id: The id of the BQ table.
  """

  bq_destination = '.'.join(
      [GOOGLE_CLOUD_PROJECT, BQ_DATASET, table_id]
  )

  if not data.empty:
    logger.info('Writing results to BQ: %s', bq_destination)
    thumbnails = data.to_dict(orient='records')
    client = bigquery.Client()

    errors = client.insert_rows_json(bq_destination, thumbnails)
    if not errors:
      logger.info('%d records written to BQ.', len(thumbnails))
    else:
      logger.error('Encountered errors while inserting rows: %s', errors)
  else:
    logger.info('Nothing to write to BQ.')
