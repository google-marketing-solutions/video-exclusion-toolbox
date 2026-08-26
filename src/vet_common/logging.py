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

import json
import logging
import os
import sys
import time
from typing import Any, Optional
import uuid


def get_service_logger(
    service_name_env: str = 'K_SERVICE',
    default_name: str = 'failed_to_get_cloud_function_name',
    log_level: int = logging.INFO,
) -> logging.Logger:
  """Initializes and returns a clean, non-duplicating service logger for Cloud Run or local testing.

  Uses standard stdout streaming which is natively captured by Cloud Run without
  introducing duplicate handlers or network latency.

  Args:
      service_name_env: Environment variable name storing the service name.
      default_name: Fallback name if service_name_env is not present.
      log_level: Logging level for the service logger.

  Returns:
      A configured standard Python Logger instance.
  """
  service_name = os.environ.get(service_name_env, default_name)
  logger = logging.getLogger(service_name)

  # Prevent multiple handlers and duplicate propagation on repeated calls/imports
  if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    logger.addHandler(handler)
    logger.propagate = False

  logger.setLevel(log_level)

  # Suppress noisy default logging from third-party client libraries.
  logging.getLogger('google_genai').setLevel(logging.WARNING)
  logging.getLogger('httpx').setLevel(logging.WARNING)
  logging.getLogger('google.ads.googleads.client').setLevel(logging.WARNING)

  return logger


class PipelineTelemetryContext:
  """Manages structured step-by-step execution telemetry for VET services."""

  def __init__(
      self,
      logger: logging.Logger,
      service_name: Optional[str] = None,
      customer_id: Optional[str] = None,
      run_id: Optional[str] = None,
  ):
    self.logger = logger
    self.service = service_name or os.environ.get(
        'K_SERVICE', 'unknown_service'
    )
    self.customer_id = customer_id
    self.run_id = run_id or str(uuid.uuid4())
    self.start_time = time.perf_counter()
    self._last_step_time = self.start_time

  def set_customer_id(self, customer_id: Optional[str]) -> None:
    """Updates the active customer_id context for subsequent steps."""
    self.customer_id = customer_id

  def log_step(
      self,
      step: str,
      status: str = 'SUCCESS',
      records_in: int = 0,
      records_out: int = 0,
      error: Optional[Exception] = None,
      metadata: Optional[dict[str, Any]] = None,
  ) -> dict[str, Any]:
    """Logs a structured JSON telemetry step to stdout for BigQuery ingestion."""
    now = time.perf_counter()
    step_duration_ms = int((now - self._last_step_time) * 1000)
    elapsed_ms = int((now - self.start_time) * 1000)
    self._last_step_time = now

    payload = {
        'event_type': 'pipeline_step',
        'run_id': self.run_id,
        'service': self.service,
        'customer_id': self.customer_id,
        'step': step,
        'status': status,
        'step_duration_ms': step_duration_ms,
        'elapsed_ms': elapsed_ms,
        'records_in': records_in,
        'records_out': records_out,
        'error_type': type(error).__name__ if error else None,
        'error_message': str(error) if error else None,
        'metadata': metadata or {},
    }

    serialized = json.dumps(payload)
    if status == 'FAILED':
      self.logger.error(serialized)
    else:
      self.logger.info(serialized)

    return payload
