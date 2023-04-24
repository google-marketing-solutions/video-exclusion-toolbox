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
from google.cloud.storage import blob
import jsonschema
import pandas as pd
import PIL.Image
import requests
from utils import bq
from utils import pubsub


logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Governs whether object crop-outs will be genearted for the thumbnail.
CROP_AND_STORE_OBJECTS = True
# Selector of what labels of objects will be cropped out.
# For age recognition, 'Face' is recommended.
OBJECTS_TO_CROP_AND_STORE = ['Face', 'Person']

# The Google Cloud project containing the pub/sub topic
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
# The name of the BigQuery Dataset.
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The bucket to write the data to.
THUMBNAIL_CROP_BUCKET = os.environ.get('VID_EXCL_THUMBNAIL_CROP_BUCKET')
# Governs whether a message with fie addess is generated once a crop-out
# image is produced.
SEND_CROPOUTS_TO_PUBSUB = os.environ.get('SEND_CROPOUTS_TO_PUBSUB')
# The topic to send the messages with cropped images po further process.
CROPPED_IMAGES_PUBSUB_TOPIC = os.environ.get('CROPPED_IMAGES_PUBSUB_TOPIC')

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

# The schema of the JSON in the event payload.
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
  thumbnails = [
      _get_best_resolution_thumbnail(THUMBNAIL_SET_DEFAULT, video_id),
      _get_best_resolution_thumbnail(THUMBNAIL_SET_1, video_id),
      _get_best_resolution_thumbnail(THUMBNAIL_SET_2, video_id),
      _get_best_resolution_thumbnail(THUMBNAIL_SET_3, video_id),
  ]

  for thumbnail in thumbnails:
    _process_thumbnail(
        thumbnail=thumbnail,
        video_id=video_id,
        generate_cropouts=CROP_AND_STORE_OBJECTS,
        cropout_object_list=OBJECTS_TO_CROP_AND_STORE,
    )


def _process_thumbnail(
    thumbnail: dict[PIL.Image.Image | str] | None,
    video_id: str,
    generate_cropouts: bool,
    cropout_object_list: list[str],
) -> None:
  """Orchestrate the full processing pipeline for a single thumbnail.

  Args:
    thumbnail: The dictionary containing the image object and its url.
    video_id: The YouTube video ID .
    generate_cropouts: The switch to control whether identified object should be
      cropped out.
    cropout_object_list: The list of object labels to be cropped out, e.g.
      ['Person', 'Car', 'Face']. Only used if generate_cropouts == True.
  """
  if thumbnail is not None:
    logger.info('Processing thumbnail: %s', thumbnail['url'])

    objects = _localized_object_annotations_from_image(thumbnail['image'])
    objects.extend(_face_annotations_from_image(thumbnail['image']))

    objects = pd.DataFrame(objects)

    objects.insert(0, 'thumbnail_url', thumbnail['url'])
    objects.insert(0, 'video_id', video_id)
    objects['datetime_updated'] = datetime.datetime.now()

    write_results_to_bq(
        objects, BQ_DATASET + '.YouTubeThumbnailsWithAnnotations'
    )

    if generate_cropouts and cropout_object_list:
      objects_to_check = objects[
          objects['label'].isin(cropout_object_list)
      ]

      for _, row in objects_to_check.iterrows():
        cropped_image = _cropout_from_image(
            image=thumbnail['image'],
            top_left_x=row['top_left_x'],
            top_left_y=row['top_left_y'],
            bottom_right_x=row['bottom_right_x'],
            bottom_right_y=row['bottom_right_y'],
        )

        image_blob = _save_image_to_gcs(
            client=storage.Client(),
            image=cropped_image,
            image_name=_generate_thumbnail_name(
                video_id=video_id,
                video_url=thumbnail['url'],
                label=row['label'],
            ),
            bucket_name=THUMBNAIL_CROP_BUCKET,
        )

        if SEND_CROPOUTS_TO_PUBSUB:
          pubsub.send_message_to_pubsub(
              message=image_blob.public_url,
              topic=CROPPED_IMAGES_PUBSUB_TOPIC,
              gcp_project=GOOGLE_CLOUD_PROJECT,
          )


def _generate_thumbnail_name(video_id: str, video_url: str, label: str) -> str:
  """Generates a sinitized file name for a GCS blob.

  Args:
    video_id: YouTube video ID, acts as a GCP folder.
    video_url: YouTube Video url.
    label: Object label.

  Returns:
    New object name for GCS in format:
    {video_id}/{label}-{random-UUID}-{sanitized video_id}.
  """
  sanitized_url = video_url
  for ch in [':', '/', '.', '?', '#', '&', '=', '+']:
    sanitized_url = sanitized_url.replace(ch, '_')
  # The "_" is for conciseness and readability purposes only, there's no
  # technical requirement, and so a simplictic replacement is sufficient.
  sanitized_url = sanitized_url.replace('___', '_').replace('__', '_')
  return f'{video_id}/{label}-{str(uuid.uuid4())[-6:]}-{sanitized_url}.png'


def _save_image_to_gcs(
    client: storage.Client, image: PIL.Image, image_name: str, bucket_name: str
) -> blob.Blob:
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
  return image_blob


def _cropout_from_image(
    image: PIL.Image.Image,
    top_left_x,
    top_left_y,
    bottom_right_x,
    bottom_right_y,
) -> PIL.Image.Image:
  """Crop an image based on relative coordinates coordinates.

  Args:
      image: The original image to crop a cuatout from.
      top_left_x (int): The x-coordinate of the top-left corner of the crop
        rectangle, as a percentage of the width of the image.
      top_left_y (int): The y-coordinate of the top-left corner of the crop
        rectangle, as a percentage of the height of the image.
      bottom_right_x (int): The x-coordinate of the bottom-right corner of the
        crop rectangle, as a percentage of the width of the image.
      bottom_right_y (int): The y-coordinate of the bottom-right corner of the
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


def get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials


def _localized_object_annotations_from_image(
    image: PIL.Image.Image,
) -> list[dict[str]]:
  """Get labels from the Google Vision API for an image object.

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
  """Get labels from the Google Vision API for an image object.

  Args:
      image: The image object to retrieve faces for.

  Returns:
      list: A list of dictionaries containing annotations for each
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


def _parse_face_annotations(vision_object: vision.FaceAnnotation) -> dict[str]:
  """Get labels from the Google Vision API for an image at the given URL.

  Args:
      vision_object: An object from the Vision API.
        (https://cloud.google.com/python/docs/reference/vision/latest/google.cloud.vision_v1.types.FaceAnnotation)

  Returns:
      dict: A dictionary of the parsed object values.
  """
  return {
      'label': 'Face',
      'confidence': vision_object.detection_confidence,
      'top_left_x': vision_object.bounding_poly.vertices[0].x,
      'top_left_y': vision_object.bounding_poly.vertices[0].y,
      'bottom_right_x': vision_object.bounding_poly.vertices[2].x,
      'bottom_right_y': vision_object.bounding_poly.vertices[2].y,
  }


def _get_best_resolution_thumbnail(
    file_names: list[str], video_id: str
) -> dict[str | PIL.Image.Image] | None:
  """Write the YouTube dataframe to BQ.

  Args:
      file_names: The an ordered list of thumbnail filenames to check.
      video_id: The id of YouTube video.

  Returns:
      A dictionary with an Image object and url of the thumbnail.
  """
  logger.info('Getting the best resolution for %s', file_names)

  for file_name in file_names:
    url = THUMBNAIL_URL.format(video_id=video_id, thumbnail_name=file_name)

    response = requests.get(url, stream=True)
    if requests.get(url).status_code == 200:
      logger.info('Best resolution was found at %s', url)
      return {'image': PIL.Image.open(response.raw), 'url': url}
  logger.info('Did not find a usable thumbnail for video %s', video_id)
  return None


def write_results_to_bq(
    youtube_df: pd.DataFrame, table_id: str
) -> None:
  """Write the YouTube dataframe to BQ.

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
