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

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "project" {
}

# SERVICE ACCOUNT --------------------------------------------------------------
resource "google_service_account" "service_account" {
  account_id   = "video-exclusion-runner"
  display_name = "Service Account for running the Video Exclusion Toolbox"
}
resource "google_project_iam_member" "cloud_functions_invoker_role" {
  project = var.project_id
  role    = "roles/cloudfunctions.invoker"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}
resource "google_project_iam_member" "bigquery_job_user_role" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}
resource "google_project_iam_member" "bigquery_data_viewer_role" {
  project = var.project_id
  role    = "roles/bigquery.dataOwner"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}
resource "google_project_iam_member" "pubsub_publisher_role" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}
resource "google_project_iam_member" "storage_object_admin_role" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}
resource "google_project_iam_member" "secret_accessor_role" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

# CLOUD_SCHEDULER --------------------------------------------------------------
locals {
  scheduler_body = <<EOF
    {
        "sheet_id": "${var.config_sheet_id}"
    }
    EOF
}

resource "google_cloud_scheduler_job" "video_exclusion_scheduler" {
  name             = "video_exclusion_toolbox"
  description      = "Run the Video Exclusion Toolbox pipeline"
  schedule         = "0 * * * *"
  time_zone        = "Etc/UTC"
  attempt_deadline = "320s"
  region           = var.region

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions_function.google_ads_accounts_function.https_trigger_url
    body        = base64encode(local.scheduler_body)
    headers = {
      "Content-Type" = "application/json"
    }
    oidc_token {
      service_account_email = google_service_account.service_account.email
    }
  }
}
