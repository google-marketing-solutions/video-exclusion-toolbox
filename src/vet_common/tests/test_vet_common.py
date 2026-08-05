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

"""Unit tests for vet_common shared utilities."""

from concurrent import futures
import os
from unittest import mock

from google.cloud import pubsub_v1
from vet_common.ids import sanitize_gads_id
from vet_common.logging import get_service_logger
from vet_common.pubsub import publish_batch


def test_sanitize_gads_id():
  """Test sanitizing Google Ads customer IDs."""
  assert sanitize_gads_id('123-456-7890') == '1234567890'
  assert sanitize_gads_id(1234567890) == '1234567890'
  assert sanitize_gads_id('  123-456-7890  ') == '1234567890'


def test_get_service_logger_local_test():
  """Test logger configuration in local test environment."""
  with mock.patch.dict(
      os.environ, {'IS_LOCAL_TEST': 'True', 'K_SERVICE': 'test-service'}
  ):
    logger = get_service_logger()
    assert logger.name == 'test-service'


@mock.patch.object(pubsub_v1, 'PublisherClient', autospec=True)
def test_publish_batch(mock_publisher_client_cls):
  """Test publishing a batch of messages to Pub/Sub."""
  mock_publisher = mock_publisher_client_cls.return_value
  mock_publisher.topic_path.return_value = (
      'projects/test-project/topics/test-topic'
  )

  mock_future = futures.Future()
  mock_future.set_result('msg-id-123')
  mock_publisher.publish.return_value = mock_future

  messages = [{'foo': 'bar'}, {'baz': 123}]
  publish_batch('test-project', 'test-topic', messages)

  assert mock_publisher.publish.call_count == 2
