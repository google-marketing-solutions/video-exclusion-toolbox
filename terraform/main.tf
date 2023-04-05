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

# CLOUD STORAGE ----------------------------------------------------------------
resource "google_storage_bucket" "video_exclusion_data_bucket" {
  name                        = "${var.project_id}-vid-excl-data"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}
# This bucket is used to store the cloud functions for deployment.
# The project ID is used to make sure the name is globally unique
resource "google_storage_bucket" "function_bucket" {
  name                        = "${var.project_id}-functions"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}
resource "google_storage_bucket" "categories_lookup" {
  name                        = "${var.project_id}-categories-lookup"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# CLOUD FUNCTIONS --------------------------------------------------------------
data "archive_file" "google_ads_accounts_zip" {
  type        = "zip"
  output_path = ".temp/google_ads_accounts_source.zip"
  source_dir  = "../src/google_ads_accounts"
}
data "archive_file" "google_ads_exclusions_zip" {
  type        = "zip"
  output_path = ".temp/google_ads_exclusions_source.zip"
  source_dir  = "../src/google_ads_exclusions"
}
data "archive_file" "google_ads_report_video_zip" {
  type        = "zip"
  output_path = ".temp/google_ads_report_video_source.zip"
  source_dir  = "../src/google_ads_report_video"
}
data "archive_file" "google_ads_report_channel_zip" {
  type        = "zip"
  output_path = ".temp/google_ads_report_channel_source.zip"
  source_dir  = "../src/google_ads_report_channel"
}
data "archive_file" "youtube_channel_zip" {
  type        = "zip"
  output_path = ".temp/youtube_channel_source.zip"
  source_dir  = "../src/youtube_channel/"
}
data "archive_file" "youtube_video_zip" {
  type        = "zip"
  output_path = ".temp/youtube_video_source.zip"
  source_dir  = "../src/youtube_video/"
}
# data "archive_file" "google_ads_excluder_zip" {
#   type        = "zip"
#   output_path = ".temp/google_ads_excluder_source.zip"
#   source_dir  = "../src/google_ads_excluder/"
# }

resource "google_storage_bucket_object" "google_ads_accounts" {
  name       = "google_ads_accounts_${data.archive_file.google_ads_accounts_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.google_ads_accounts_zip.output_path
  depends_on = [data.archive_file.google_ads_accounts_zip]
}
resource "google_storage_bucket_object" "google_ads_exclusions" {
  name       = "google_ads_exclusions_${data.archive_file.google_ads_exclusions_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.google_ads_exclusions_zip.output_path
  depends_on = [data.archive_file.google_ads_exclusions_zip]
}
resource "google_storage_bucket_object" "google_ads_report_video" {
  name       = "google_ads_report_video${data.archive_file.google_ads_report_video_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.google_ads_report_video_zip.output_path
  depends_on = [data.archive_file.google_ads_report_video_zip]
}
resource "google_storage_bucket_object" "google_ads_report_channel" {
  name       = "google_ads_report_channel${data.archive_file.google_ads_report_channel_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.google_ads_report_channel_zip.output_path
  depends_on = [data.archive_file.google_ads_report_channel_zip]
}
resource "google_storage_bucket_object" "youtube_channel" {
  name       = "youtube_channel_${data.archive_file.youtube_channel_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.youtube_channel_zip.output_path
  depends_on = [data.archive_file.youtube_channel_zip]
}
resource "google_storage_bucket_object" "youtube_video" {
  name       = "youtube_video_${data.archive_file.youtube_video_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.youtube_video_zip.output_path
  depends_on = [data.archive_file.youtube_video_zip]
}
# resource "google_storage_bucket_object" "google_ads_excluder" {
#   name       = "google_ads_excluder_${data.archive_file.google_ads_excluder_zip.output_md5}.zip"
#   bucket     = google_storage_bucket.function_bucket.name
#   source     = data.archive_file.google_ads_excluder_zip.output_path
#   depends_on = [data.archive_file.google_ads_excluder_zip]
# }
resource "google_storage_bucket_object" "categories_lookup" {
  name       = "categories_lookup.csv"
  bucket     = google_storage_bucket.categories_lookup.name
  source     = "../src/categories_lookup.csv"
  content_type = "text/plain"
}

resource "google_cloudfunctions_function" "google_ads_accounts_function" {
  region                = var.region
  name                  = "vid-excl-google_ads_accounts"
  description           = "Identify which reports to run the Google Ads report for."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.google_ads_accounts.name
  service_account_email = google_service_account.service_account.email
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"
  trigger_http          = true

  environment_variables = {
    GOOGLE_CLOUD_PROJECT                      = var.project_id
    VID_EXCL_ADS_REPORT_VIDEO_PUBSUB_TOPIC    = google_pubsub_topic.google_ads_report_video_pubsub_topic.name
    VID_EXCL_ADS_REPORT_CHANNEL_PUBSUB_TOPIC  = google_pubsub_topic.google_ads_report_channel_pubsub_topic.name
    VID_EXCL_ADS_EXCLUSIONS_PUBSUB_TOPIC      = google_pubsub_topic.google_ads_exclusions_pubsub_topic.name

  }
}
resource "google_cloudfunctions_function" "google_ads_exclusions_function" {
  region                = var.region
  name                  = "vid-excl-google_ads_exclusions"
  description           = "Retrieve all the excluded videos and channels for a given account and store them in BigQuery."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.google_ads_exclusions.name
  service_account_email = google_service_account.service_account.email
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.google_ads_exclusions_pubsub_topic.name
  }

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS       = false
    GOOGLE_ADS_REFRESH_TOKEN        = var.oauth_refresh_token
    GOOGLE_ADS_CLIENT_ID            = var.google_cloud_client_id
    GOOGLE_ADS_CLIENT_SECRET        = var.google_cloud_client_secret
    GOOGLE_ADS_DEVELOPER_TOKEN      = var.google_ads_developer_token
    GOOGLE_ADS_LOGIN_CUSTOMER_ID    = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT            = var.project_id
    VID_EXCL_GCS_DATA_BUCKET        = google_storage_bucket.video_exclusion_data_bucket.name
  }
}
resource "google_cloudfunctions_function" "google_ads_report_video_function" {
  region                = var.region
  name                  = "vid-excl-google_ads_report_video"
  description           = "Move the placement report from Google Ads to BigQuery."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.google_ads_report_video.name
  service_account_email = google_service_account.service_account.email
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.google_ads_report_video_pubsub_topic.name
  }

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS           = false
    GOOGLE_ADS_REFRESH_TOKEN            = var.oauth_refresh_token
    GOOGLE_ADS_CLIENT_ID                = var.google_cloud_client_id
    GOOGLE_ADS_CLIENT_SECRET            = var.google_cloud_client_secret
    GOOGLE_ADS_DEVELOPER_TOKEN          = var.google_ads_developer_token
    GOOGLE_ADS_LOGIN_CUSTOMER_ID        = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT                = var.project_id
    VID_EXCL_GCS_DATA_BUCKET            = google_storage_bucket.video_exclusion_data_bucket.name
    VID_EXCL_YOUTUBE_VIDEO_PUBSUB_TOPIC = google_pubsub_topic.youtube_video_pubsub_topic.name
  }
}
resource "google_cloudfunctions_function" "google_ads_report_channel_function" {
  region                = var.region
  name                  = "vid-excl-google_ads_report_channel"
  description           = "Move the channel placement report from Google Ads to BigQuery."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.google_ads_report_channel.name
  service_account_email = google_service_account.service_account.email
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.google_ads_report_channel_pubsub_topic.name
  }

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS            = false
    GOOGLE_ADS_REFRESH_TOKEN              = var.oauth_refresh_token
    GOOGLE_ADS_CLIENT_ID                  = var.google_cloud_client_id
    GOOGLE_ADS_CLIENT_SECRET              = var.google_cloud_client_secret
    GOOGLE_ADS_DEVELOPER_TOKEN            = var.google_ads_developer_token
    GOOGLE_ADS_LOGIN_CUSTOMER_ID          = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT                  = var.project_id
    VID_EXCL_GCS_DATA_BUCKET              = google_storage_bucket.video_exclusion_data_bucket.name
    VID_EXCL_YOUTUBE_CHANNEL_PUBSUB_TOPIC = google_pubsub_topic.youtube_channel_pubsub_topic.name
  }
}

resource "google_cloudfunctions_function" "youtube_channel_function" {
  region                = var.region
  name                  = "vid-excl-youtube_channels"
  description           = "Pull the channel data from the YouTube API."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.youtube_channel.name
  service_account_email = google_service_account.service_account.email
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_channel_pubsub_topic.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT                = var.project_id
    VID_EXCL_ADS_EXCLUDER_PUBSUB_TOPIC  = google_pubsub_topic.google_ads_excluder_pubsub_topic.name
    VID_EXCL_BIGQUERY_DATASET           = google_bigquery_dataset.dataset.dataset_id
    VID_EXCL_GCS_DATA_BUCKET            = google_storage_bucket.video_exclusion_data_bucket.name
  }
}

resource "google_cloudfunctions_function" "youtube_video_function" {
  region                = var.region
  name                  = "vid-excl-youtube_videos"
  description           = "Pull the video data from the YouTube API."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.youtube_video.name
  service_account_email = google_service_account.service_account.email
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_video_pubsub_topic.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT                = var.project_id
    VID_EXCL_ADS_EXCLUDER_PUBSUB_TOPIC  = google_pubsub_topic.google_ads_excluder_pubsub_topic.name
    VID_EXCL_BIGQUERY_DATASET           = google_bigquery_dataset.dataset.dataset_id
    VID_EXCL_GCS_DATA_BUCKET            = google_storage_bucket.video_exclusion_data_bucket.name
  }
}


# resource "google_cloudfunctions_function" "google_ads_excluder_function" {
#   region                = var.region
#   name                  = "vid-excl-google_ads_excluder"
#   description           = "Exclude the channels in Google Ads"
#   runtime               = "python310"
#   source_archive_bucket = google_storage_bucket.function_bucket.name
#   source_archive_object = google_storage_bucket_object.google_ads_excluder.name
#   service_account_email = google_service_account.service_account.email
#   timeout               = 540
#   available_memory_mb   = 1024
#   entry_point           = "main"

#   event_trigger {
#     event_type     = "providers/cloud.pubsub/eventTypes/topic.publish"
#     resource       = google_pubsub_topic.google_ads_excluder_pubsub_topic.name
#   }

#   environment_variables = {
#     GOOGLE_CLOUD_PROJECT              = var.project_id
#     GOOGLE_ADS_USE_PROTO_PLUS         = false
#     GOOGLE_ADS_REFRESH_TOKEN          = var.oauth_refresh_token
#     GOOGLE_ADS_CLIENT_ID              = var.google_cloud_client_id
#     GOOGLE_ADS_CLIENT_SECRET          = var.google_cloud_client_secret
#     GOOGLE_ADS_DEVELOPER_TOKEN        = var.google_ads_developer_token
#     GOOGLE_ADS_LOGIN_CUSTOMER_ID      = var.google_ads_login_customer_id
#     VID_EXCL_BIGQUERY_DATASET         = google_bigquery_dataset.dataset.dataset_id
#     VID_EXCL_GCS_DATA_BUCKET          = google_storage_bucket.video_exclusion_data_bucket.name
#   }
# }

# PUB/SUB ----------------------------------------------------------------------
resource "google_pubsub_topic" "google_ads_report_video_pubsub_topic" {
  name                       = "vid-excl-google-ads-report-video_topic"
  message_retention_duration = "604800s"
}
resource "google_pubsub_topic" "google_ads_exclusions_pubsub_topic" {
  name                       = "vid-excl-google-ads-exclusions_topic"
  message_retention_duration = "604800s"
}
resource "google_pubsub_topic" "google_ads_report_channel_pubsub_topic" {
  name                       = "vid-excl-google-ads-report-channel_topic"
  message_retention_duration = "604800s"
}
resource "google_pubsub_topic" "youtube_video_pubsub_topic" {
  name                       = "vid-excl-youtube-video-topic"
  message_retention_duration = "604800s"
}
resource "google_pubsub_topic" "youtube_channel_pubsub_topic" {
  name                       = "vid-excl-youtube-channel-topic"
  message_retention_duration = "604800s"
}
resource "google_pubsub_topic" "google_ads_excluder_pubsub_topic" {
  name                       = "vid-excl-google-ads-excluder-topic"
  message_retention_duration = "604800s"
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
