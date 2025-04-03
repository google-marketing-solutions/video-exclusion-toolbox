# Copyright 2025 Google LLC
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

resource "google_cloudfunctions2_function" "youtube_thumbnails_evaluate_age_dispatcher" {
  location              = var.region
  name                  = "vet-thumbnail-age-evaluation-dispatcher"
  description           = "Dispatch videos for thumbnail age evaluation."

  service_config {
    max_instance_count  = 2
    min_instance_count  = 1
    available_memory    = "1Gi"
    timeout_seconds     = 600
    available_cpu       = "1"
    max_instance_request_concurrency = 1
    environment_variables = {
        GOOGLE_CLOUD_PROJECT                = var.project_id
        VET_THUMBNAIL_AGE_EVALUATION_TOPIC  = google_pubsub_topic.youtube_thumbnails_to_evaluate_age.name
        VET_BIGQUERY_SOURCE_DATASET         = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
        VET_BIGQUERY_TARGET_DATASET         = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
        VET_BIGQUERY_SOURCE_TABLE           = google_bigquery_table.youtube_video.table_id
        VET_BIGQUERY_TARGET_TABLE           = google_bigquery_table.youtube_thumbnail_age_evaluation.table_id
    }
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = google_service_account.video_exclusion_toolbox.email
    all_traffic_on_latest_revision = true
  }

  build_config {
    runtime     = "python312"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.source_archive.name
        object = google_storage_bucket_object.youtube_thumbnails_evaluate_age_dispatcher.name
      }
    }
    service_account = google_service_account.video_exclusion_toolbox.id
  }

  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_thumbnails_evaluate_age_dispatcher
  ]
}

resource "google_cloudfunctions2_function" "youtube_thumbnails_evaluate_age_processor" {
  location    = var.region
  name        = "vet-thumbnail-age-evaluation-processor"
  description = "Process videos for thumbnail age evaluation."

  service_config {
    max_instance_count  = 200
    min_instance_count  = 0
    available_memory    = "1Gi"
    timeout_seconds     = 540
    available_cpu       = "1"
    max_instance_request_concurrency = 1
    environment_variables = {
        GOOGLE_CLOUD_PROJECT        = var.project_id
        VET_BIGQUERY_TARGET_DATASET = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
        VET_BIGQUERY_TARGET_TABLE   = google_bigquery_table.youtube_thumbnail_age_evaluation.table_id
        VET_GEMINI_LOCATION         = var.age_evaluation_gemini_location
        VET_GEMINI_MODEL            = var.age_evaluation_gemini_model
    }
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = google_service_account.video_exclusion_toolbox.email
    all_traffic_on_latest_revision = true
  }

  event_trigger {
    trigger_region  = var.region
    event_type      = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic    = google_pubsub_topic.youtube_thumbnails_to_evaluate_age.id
    retry_policy    = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.video_exclusion_toolbox.email
  }

  build_config {
    runtime     = "python312"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.source_archive.name
        object = google_storage_bucket_object.youtube_thumbnails_evaluate_age_processor.name
      }
    }
    service_account = google_service_account.video_exclusion_toolbox.id
  }

  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_thumbnails_evaluate_age_processor
  ]
}