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

"""Asynchronous batch publishing helper for Google Cloud Pub/Sub."""

from collections.abc import Mapping, Sequence
from concurrent import futures
import functools
import json
import logging
from typing import Any, Optional

from google.cloud import pubsub_v1


def publish_batch(
    project_id: str,
    topic_id: str,
    messages: Sequence[Mapping[str, Any]],
    logger: Optional[logging.Logger] = None,
    max_messages: int = 100,
) -> None:
  """Publishes multiple messages to a Pub/Sub topic with batch settings.

  Args:
      project_id: The Google Cloud Project containing the Pub/Sub topic.
      topic_id: The name of the topic to publish messages to.
      messages: A sequence of message mapping payloads to publish.
      logger: Optional Logger instance to log publishing progress and errors.
      max_messages: Maximum number of messages per batch.
  """
  if not logger:
    logger = logging.getLogger(__name__)

  batch_settings = pubsub_v1.types.BatchSettings(max_messages=max_messages)
  publisher = pubsub_v1.PublisherClient(batch_settings)
  topic_path = publisher.topic_path(project_id, topic_id)
  publish_futures: list[futures.Future[str]] = []

  def callback(
      data: bytes,
      current: int,
      total: int,
      topic_path_str: str,
      future: futures.Future[str],
  ) -> None:
    if future.exception():
      logger.info(
          'Failed to publish %s to %s, exception:\n%s.',
          data.decode('UTF-8'),
          topic_path_str,
          str(future.exception()),
      )
    else:
      logger.info(
          'Message %s (%d/%d) published to %s.',
          data.decode('UTF-8'),
          current,
          total,
          topic_path_str,
      )

  for current, data_item in enumerate(messages):
    message_str = json.dumps(data_item)
    data = message_str.encode('utf-8')
    publish_future = publisher.publish(topic_path, data)
    publish_future.add_done_callback(
        functools.partial(
            callback, data, current + 1, len(messages), topic_path
        )
    )
    publish_futures.append(publish_future)

  futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

  logger.info(
      'Finished publishing %d messages as a batch to %s.',
      len(messages),
      topic_path,
  )
