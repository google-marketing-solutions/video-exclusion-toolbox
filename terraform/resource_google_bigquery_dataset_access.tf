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

resource "google_bigquery_dataset_access" "pipeline_stats_sink_bq_writer" {
  project       = var.project_id
  dataset_id    = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = replace(google_logging_project_sink.pipeline_stats.writer_identity, "serviceAccount:", "")
  depends_on    = [google_logging_project_sink.pipeline_stats]
}
