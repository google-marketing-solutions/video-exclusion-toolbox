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

"""Pull YouTube video data for the placements in the Google Ads report."""
import base64
import datetime
import io
import json
import logging
import os
import sys
from typing import Any, Dict
import uuid

import google.auth
import google.auth.credentials
from google.cloud import storage
from google.cloud import vision
from google.cloud.storage.blob import Blob
import jsonschema
import pandas as pd
import PIL.Image
import requests
from utils import bq

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CROP_AND_STORE_OBJECTS = True
OBJECTS_TO_CROP_AND_STORE = ['Person']

# The name of the BigQuery Dataset
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The bucket to write the data to
THUMBNAIL_CROP_BUCKET = os.environ.get('THUMBNAIL_CROP_BUCKET')

THUMBNAIL_URL = 'https://i.ytimg.com/vi/{video_id}/{thumbnail_name}.jpg'

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

# The schema of the JSON in the event payload
message_schema = {
    'type': 'object',
    'properties': {
        'video_id': {'type': 'string'}
    },
    'required': [
        'video_id'
    ],
}


def main(event: Dict[str, Any], context: Dict[str, Any]) -> None:
  """The entry point: extract the data from the payload and starts the job.

  The pub/sub message must match the message_schema object above.

  Args:
      event: A dictionary representing the event data payload.
      context: An object containing metadata about the event.

  Raises:
      jsonschema.exceptions.ValidationError if the message from pub/sub is not
      what is expected.
  """
  del context
  logger.info('YouTube video service triggered.')
  logger.info('Message: %s', event)
  message = base64.b64decode(event['data']).decode('utf-8')
  logger.info('Decoded message: %s', message)
  message_json = json.loads(message)
  logger.info('JSON message: %s', message_json)

  # Will raise jsonschema.exceptions.ValidationError if the schema is invalid
  jsonschema.validate(instance=message_json, schema=message_schema)

  run(message_json.get('video_id'))

  logger.info('Done')


def run(video_id: str) -> None:
  """Orchestration to pull YouTube data and output it to BigQuery.

  Args:
      video_id: The Google Ads customer ID to process.
  """

  logger.info('Looking up thumbnails for video %s', video_id)
  thumbnail_default = _get_best_resolution(THUMBNAIL_SET_DEFAULT, video_id)
  thumbnail_1 = _get_best_resolution(THUMBNAIL_SET_1, video_id)
  thumbnail_2 = _get_best_resolution(THUMBNAIL_SET_2, video_id)
  thumbnail_3 = _get_best_resolution(THUMBNAIL_SET_3, video_id)

  all_thumbnails = []
  if thumbnail_default:
    logger.info('Analyzing objects in %s', thumbnail_default)
    all_thumbnails.append(
        _get_vision_api_labels_to_df(thumbnail_default, video_id)
    )
  if thumbnail_1:
    logger.info('Analyzing objects in %s', thumbnail_1)
    all_thumbnails.append(_get_vision_api_labels_to_df(thumbnail_1, video_id))
  if thumbnail_2:
    logger.info('Analyzing objects in %s', thumbnail_2)
    all_thumbnails.append(_get_vision_api_labels_to_df(thumbnail_2, video_id))
  if thumbnail_3:
    logger.info('Analyzing objects in %s', thumbnail_3)
    all_thumbnails.append(_get_vision_api_labels_to_df(thumbnail_3, video_id))

  all_thumbnails = pd.concat(all_thumbnails, axis=0, ignore_index=True)

  write_results_to_bq(
      all_thumbnails, BQ_DATASET + '.YouTubeThumbnailsWithAnnotations'
  )

  if CROP_AND_STORE_OBJECTS:
    objects_to_check = all_thumbnails[
        all_thumbnails['label'].isin(OBJECTS_TO_CROP_AND_STORE)
    ]

    object_urls = []
    for _, row in objects_to_check.iterrows():
      cropped_image = _get_image_cropout(
          url=row['thumbnail_url'],
          top_left_x=row['top_left_x'],
          top_left_y=row['top_left_y'],
          bottom_right_x=row['bottom_right_x'],
          bottom_right_y=row['bottom_right_y'],
      )
      blob = _save_image_to_gcs(
          client=storage.Client(),
          image=cropped_image,
          image_name=(
              f'{row["video_id"]}/{row["label"]}-{str(uuid.uuid4())[-6:]}.png'
          ),
          bucket_name=THUMBNAIL_CROP_BUCKET,
      )
      object_urls.append(blob.public_url)
    # TODO(jakubmedved) send the urls to a pubsub for further processing


def _save_image_to_gcs(
    client: storage.Client, image: PIL.Image, image_name: str, bucket_name: str
) -> Blob:
  """Uploads an image object to a GCS bucket.

  Args:
    client: An initialized GCS client
    image: Image object
    image_name: Name of the object to be created as
    bucket_name: Name oth the GCS bucket for the object to be created in

  Returns:
    Url of the newly created GCS object
  """

  bucket = client.bucket(bucket_name)
  img_byte_array = io.BytesIO()
  image.save(img_byte_array, format='JPEG')
  image_blob = bucket.blob(image_name)
  image_blob.upload_from_string(
      img_byte_array.getvalue(), content_type='image/jpeg'
  )
  return image_blob


def _get_image_cropout(
    url: str,
    top_left_x,
    top_left_y,
    bottom_right_x,
    bottom_right_y,
) -> PIL.Image:
  """Crops an image based on the given relative coordinates and uploads the resulting crop-out to a cloud bucket.

  Args:
      url (str): The URL of the image to crop.
      top_left_x (int): The x-coordinate of the top-left corner of the crop
        rectangle, as a percentage of the width of the image.
      top_left_y (int): The y-coordinate of the top-left corner of the crop
        rectangle, as a percentage of the height of the image.
      bottom_right_x (int): The x-coordinate of the bottom-right corner of the
        crop rectangle, as a percentage of the width of the image.
      bottom_right_y (int): The y-coordinate of the bottom-right corner of the
        crop rectangle, as a percentage of the height of the image.

  Returns:
      PIL.Image: The cropped image.
  """

  image = PIL.Image.open(requests.get(url, stream=True).raw)
  width = image.width
  height = image.height

  top_left_x = top_left_x * width
  top_left_y = top_left_y * height
  bottom_right_x = bottom_right_x * width
  bottom_right_y = bottom_right_y * height

  cropped_image = image.crop(
      (top_left_x, top_left_y, bottom_right_x, bottom_right_y)
  )

  return cropped_image


def get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials


def _get_vision_api_labels_to_df(image_url: str, video_id: str) -> pd.DataFrame:
  """Get labels from the Google Vision API for an image at the given URL.

  Args:
      image_url: The URL of the image.
      video_id: The ID of the YouTube video.

  Returns:
      DataFrame: A DataFrame with object names, confidence scores and
        coordinates.
  """

  client = vision.ImageAnnotatorClient()

  image = vision.Image()
  image.source.image_uri = image_url

  objects = client.object_localization(image=image).localized_object_annotations

  logger.info('Number of objects found: %s', len(objects))
  parsed_objects = []
  for object_ in objects:
    parsed_objects.append(_parse_vision_object_annotations(object_))

  parsed_objects_df = pd.DataFrame(parsed_objects)
  parsed_objects_df.insert(0, 'thumbnail_url', image_url)
  parsed_objects_df.insert(0, 'video_id', video_id)
  parsed_objects_df['datetime_updated'] = datetime.datetime.now()
  return parsed_objects_df


def _parse_vision_object_annotations(
    vision_object: vision.LocalizedObjectAnnotation) -> dict[str]:
  """Get labels from the Google Vision API for an image at the given URL.

  Args:
      vision_object: An object from the Vision API.
      (https://cloud.google.com/python/docs/reference/vision/latest/google.cloud.vision_v1.types.LocalizedObjectAnnotation)

  Returns:
      dict: A dictionary of the parsed object values.
  """

  return {
      'label': vision_object.name,
      'confidence': vision_object.score,
      'top_left_x': vision_object.bounding_poly.normalized_vertices[0].x,
      'top_left_y': vision_object.bounding_poly.normalized_vertices[0].y,
      'bottom_right_x': vision_object.bounding_poly.normalized_vertices[2].x,
      'bottom_right_y': vision_object.bounding_poly.normalized_vertices[2].y,
  }


def _get_best_resolution(names: list[str], video_id: str) -> str | None:
  """Writes the YouTube dataframe to BQ.

  Args:
      names: The an ordered list of thumbnail filenames to check.
      video_id: The id of YouTube video.

  Returns:
      The url with the best resolution tumbnail available.
  """

  for name in names:
    url = THUMBNAIL_URL.format(video_id=video_id, thumbnail_name=name)
    if requests.get(url) == 200:
      return url
  return None


def write_results_to_bq(
    youtube_df: pd.DataFrame, table_id: str
) -> None:
  """Writes the YouTube dataframe to BQ.

  Args:
      youtube_df: The dataframe based on the YouTube data.
      table_id: The id of the BQ table.
  """
  logger.info('Writing results to BQ: %s', table_id)
  number_of_rows = len(youtube_df.index)
  logger.info('There are %s rows', number_of_rows)
  if number_of_rows > 0:
    bq.load_to_bq_from_df(
        df=youtube_df, table_id=table_id
    )
    logger.info('YT data added to BQ table')
  else:
    logger.info('There is nothing to write to BQ')
