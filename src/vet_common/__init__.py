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

"""Shared utilities and common infrastructure for Video Exclusion Toolbox (VET) services."""

from vet_common.bq import get_existing_partition_ids
from vet_common.bq import upsert_ndjson_to_bq
from vet_common.bq import write_df_to_bq
from vet_common.bq import write_ndjson_to_bq
from vet_common.dates import get_lookback_date_range
from vet_common.events import parse_pubsub_cloudevent
from vet_common.gads import DEFAULT_GOOGLE_ADS_API_VERSION
from vet_common.gads import get_google_ads_client_version
from vet_common.ids import sanitize_gads_id
from vet_common.logging import get_service_logger
from vet_common.pubsub import publish_batch

__all__ = [
    'DEFAULT_GOOGLE_ADS_API_VERSION',
    'get_existing_partition_ids',
    'get_google_ads_client_version',
    'get_lookback_date_range',
    'get_service_logger',
    'parse_pubsub_cloudevent',
    'publish_batch',
    'sanitize_gads_id',
    'upsert_ndjson_to_bq',
    'write_df_to_bq',
    'write_ndjson_to_bq',
]
