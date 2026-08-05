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

"""Standardized Cloud Logging bootstrap for Video Exclusion Toolbox services."""

import logging
import os
import sys

from google.cloud import logging as cloud_logging


def get_service_logger(
    service_name_env: str = 'K_SERVICE',
    default_name: str = 'failed_to_get_cloud_function_name',
    log_level: int = logging.INFO,
) -> logging.Logger:
  """Initializes and returns a logger configured for Cloud Run or local testing.

  Args:
      service_name_env: Environment variable name storing the service name.
      default_name: Fallback name if service_name_env is not present.
      log_level: Logging level for the service logger.

  Returns:
      A configured standard Python Logger instance.
  """
  service_name = os.environ.get(service_name_env, default_name)
  logger = logging.getLogger(service_name)

  if os.getenv('IS_LOCAL_TEST', 'False') == 'True':
    logging.basicConfig(level=log_level, stream=sys.stdout)
  else:
    logging_client = cloud_logging.Client()
    logging_client.setup_logging(log_level=log_level)

  # Suppress noisy default logging from third-party client libraries.
  logging.getLogger('google_genai').setLevel(logging.WARNING)
  logging.getLogger('httpx').setLevel(logging.WARNING)
  logging.getLogger('google.ads.googleads.client').setLevel(logging.WARNING)

  logger.setLevel(log_level)
  return logger
