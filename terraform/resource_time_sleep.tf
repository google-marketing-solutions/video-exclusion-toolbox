# Copyright 2024 Google LLC.
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

resource "time_sleep" "wait_60_seconds_after_role_assignment" {
  create_duration = "60s"
  depends_on = [
    resource.google_project_iam_member.cloud_functions_invoker,
    resource.google_project_iam_member.bigquery_job_user,
    resource.google_project_iam_member.bigquery_data_owner,
    resource.google_project_iam_member.pubsub_publisher,
    resource.google_project_iam_member.storage_object_admin,
    resource.google_project_iam_member.secret_accessor,
    resource.google_project_iam_member.cloudbuild_builds_builder
  ]
}