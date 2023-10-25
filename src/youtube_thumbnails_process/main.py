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
import json
import logging
import os
import sys
from typing import Any

import google.auth
import google.auth.credentials
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import vision
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
BQ_DATASET = os.environ.get('VID_EXCL_BIGQUERY_DATASET')
# The topic to send video_ids to to have objects cropped from them.
THUMBNAILS_TO_GENERATE_CROPOUTS_TOPIC = os.environ.get(
    'VID_EXCL_THUMBNAILS_TO_GENERATE_CROPOUTS_TOPIC'
)

# Selector of what labels of objects will be cropped out.
# For age recognition, 'Face' and 'Person' are recommended.
OBJECTS_TO_CROP_AND_STORE = ['Face', 'Person']
# BQ Table name to store the detected object's metadata. This is not expexted
# to be configurable and so is not exposed as an environmental variable
BQ_TABLE_NAME = 'YouTubeThumbnailsWithAnnotations'
# Switch whether to store cropped thumbnail objecs in GCS.
CROP_AND_STORE_OBJECTS = os.environ.get(
    'VID_EXCL_CROP_AND_STORE_OBJECTS', 'False'
).lower() in ('true', '1', 't')

THUMBNAIL_URL_TEMPLATE = (
    'https://i.ytimg.com/vi/{video_id}/{thumbnail_name}.jpg'
)
CHARS_TO_REPLACE_IN_IMAGE_NAME = [':', '/', '.', '?', '#', '&', '=', '+']
IMAGE_FEATURE_TYPES = [
    {'type_': vision.Feature.Type.FACE_DETECTION},
    {'type_': vision.Feature.Type.OBJECT_LOCALIZATION},
    {'type_': vision.Feature.Type.LABEL_DETECTION},
]
THUMBNAIL_RESOLUTIONS = (
    (
        'maxresdefault',
        'hq720',
        'sddefault',
        'hqdefault',
        '0',
        'mqdefault',
        'default',
    ),
    ('sd1', 'hq1', 'mq1', '1'),
    ('sd2', 'hq2', 'mq2', '2'),
    ('sd3', 'hq3', 'mq3', '3'),
)

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
    logger.info('Finished processing thumbnails for video %s.', video_id)


def _process_video(video_id: str) -> None:
  """Orchestrates pulling YouTube data and output it to BigQuery.

  Args:
      video_id: The YouTube Video ID to process.
  """
  logger.info('Looking up thumbnails for video %s.', video_id)
  thumbnails = _get_best_resolution_thumbnails(video_id=video_id)

  extracted_data = []
  for url in thumbnails:
    logger.info('Processing thumbnail %s.', url)
    thumbnail_data = _extract_features_df_from_image_uri(image_uri=url)
    thumbnail_data.insert(0, 'thumbnail_url', url)
    thumbnail_data.insert(0, 'video_id', video_id)
    extracted_data.append(thumbnail_data)

  if not extracted_data:
    logger.info(
        'No usable thubmnails found for video %s. No features extracted.',
        video_id,
    )
    return

  extracted_data = pd.concat(extracted_data, ignore_index=True)

  if extracted_data.empty:
    logger.info(
        'No features extracted from thumbnails for video %s.',
        video_id,
    )
    return

  logger.info(
      'Extracted %d object(s) from %d thumbnail(s).',
      len(extracted_data.index),
      len(thumbnails),
  )

  _write_results_to_bq(
      extracted_data, BQ_TABLE_NAME
  )

  if CROP_AND_STORE_OBJECTS:
    _send_video_for_processing(
        video_id=video_id,
        topic=THUMBNAILS_TO_GENERATE_CROPOUTS_TOPIC,
        gcp_project=GOOGLE_CLOUD_PROJECT,
    )


def _extract_features_df_from_image_uri(image_uri: str) -> pd.DataFrame:
  """Extracts features from a single thumbnail url.

  Args:
    image_uri: The location of the thumbnail.

  Returns:
    A dataframe of all the features detected in the thumbnail.
  """
  client = vision.ImageAnnotatorClient()
  image = vision.Image()
  image.source.image_uri = image_uri
  features = IMAGE_FEATURE_TYPES
  request = vision.AnnotateImageRequest(image=image, features=features)
  response = client.annotate_image(request=request)

  faces = [_parse_face_annotations(face) for face in response.face_annotations]

  objects = [
      _parse_vision_object_annotations(object_annotation)
      for object_annotation in response.localized_object_annotations
  ]

  labels = [
      _parse_label_annotations(label) for label in response.label_annotations
  ]

  return pd.DataFrame(faces + objects + labels)


def _get_auth_credentials() -> google.auth.credentials.Credentials:
  """Returns credentials for Google APIs."""
  credentials, _ = google.auth.default()
  return credentials


def _parse_vision_object_annotations(
    vision_object: vision.LocalizedObjectAnnotation) -> dict[str]:
  """Gets labels from the Google Vision API for an image at the given URL.

  Args:
      vision_object: An object from the Vision API.
      (https://cloud.google.com/python/docs/reference/vision/latest/google.cloud.vision_v1.types.LocalizedObjectAnnotation)

  Returns:
      Parsed object annotations.
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


def _parse_label_annotations(
    vision_object: vision.EntityAnnotation,
) -> dict[str]:
  """Gets labels from the Google Vision API from a label annotation object.

  Args:
      vision_object: An object from the Vision API.
        (https://cloud.google.com/python/docs/reference/vision/latest/google.cloud.vision_v1.types.EntityAnnotation)

  Returns:
      Parsed label annotations.
  """
  return {
      'label': vision_object.description,
      'confidence': vision_object.score,
      'top_left_x': '0',
      'top_left_y': '0',
      'bottom_right_x': '0',
      'bottom_right_y': '0',
      'datetime_updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
  }


def _get_best_resolution_thumbnails(
    video_id: str,
) -> dict[PIL.Image.Image | None]:
  """Retrieves the best resolution of each video's thumbnails.

  Args:
      video_id: The id of YouTube video.

  Returns:
      A dictionary with the thumbnail's url as the key and an Image object as
      value. Returns an empty dictionary if no thumbnails were found.
  """
  thumbnails = {}
  for names in THUMBNAIL_RESOLUTIONS:
    for name in names:
      url = THUMBNAIL_URL_TEMPLATE.format(
          video_id=video_id, thumbnail_name=name
      )

      response = requests.get(url, stream=True)
      if requests.get(url).status_code == 200:
        logger.info('Best resolution was found at %s', url)
        thumbnails[url] = PIL.Image.open(response.raw)
        break
  if thumbnails:
    return thumbnails
  else:
    logger.info('Did not find any usable thumbnails for video %s', video_id)
    return {}


def _write_results_to_bq(data: pd.DataFrame, table_id: str) -> None:
  """Writes the YouTube dataframe to BQ.

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


def _send_video_for_processing(
    video_id: str, topic: str, gcp_project: str
) -> None:
  """Pushes the dictionary to pubsub.

  Args:
      video_id: The viseo ID to push to pubsub.
      topic: The name of the topic to publish the message to.
      gcp_project: The Google Cloud Project with the pub/sub topic in.
  """
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(gcp_project, topic)
  message_str = json.dumps({'video_id': video_id})
  # Data must be a bytestring
  data = message_str.encode('utf-8')
  publisher.publish(topic_path, data)
  logger.info(
      'Video %s dispatched for thumbnail processing to %s.',
      video_id,
      topic_path,
  )
