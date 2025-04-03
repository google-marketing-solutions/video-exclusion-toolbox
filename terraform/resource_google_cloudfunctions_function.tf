# Copyright 2024 Google LLC
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

resource "google_cloudfunctions_function" "google_ads_accounts" {
  region                = var.region
  name                  = "vid-excl-google_ads_accounts"
  description           = "Identify which reports to run the Google Ads report for."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.google_ads_accounts.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"
  trigger_http          = true
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.google_ads_accounts
  ]

  environment_variables = {
    GOOGLE_CLOUD_PROJECT              = var.project_id
    VID_EXCL_ADS_ACCOUNT_PUBSUB_TOPIC = google_pubsub_topic.google_ads_account.name
  }
}


resource "google_cloudfunctions_function" "google_ads_exclusions" {
  region                = var.region
  name                  = "vid-excl-google_ads_exclusions"
  description           = "Retrieve all the excluded videos and channels for a given account and store them in BigQuery."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.google_ads_exclusions.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.google_ads_exclusions
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.google_ads_account.name
  }

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS    = false
    GOOGLE_ADS_LOGIN_CUSTOMER_ID = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT         = var.project_id
    VID_EXCL_BIGQUERY_DATASET    = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  }

  secret_environment_variables {
    key     = "GOOGLE_ADS_REFRESH_TOKEN"
    secret  = google_secret_manager_secret.oauth_refresh_token.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_ID"
    secret  = google_secret_manager_secret.client_id.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_SECRET"
    secret  = google_secret_manager_secret.client_secret.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_DEVELOPER_TOKEN"
    secret  = google_secret_manager_secret.developer_token.secret_id
    version = "latest"
  }

}


resource "google_cloudfunctions_function" "google_ads_report_video" {
  region                = var.region
  name                  = "vid-excl-google_ads_report_video"
  description           = "Move the placement report from Google Ads to BigQuery."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.google_ads_report_video.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.google_ads_report_video
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.google_ads_account.name
  }

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS           = false
    GOOGLE_ADS_LOGIN_CUSTOMER_ID        = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT                = var.project_id
    VID_EXCL_BIGQUERY_DATASET           = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
    VID_EXCL_YOUTUBE_VIDEO_PUBSUB_TOPIC = google_pubsub_topic.youtube_video.name
  }

  secret_environment_variables {
    key     = "GOOGLE_ADS_REFRESH_TOKEN"
    secret  = google_secret_manager_secret.oauth_refresh_token.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_ID"
    secret  = google_secret_manager_secret.client_id.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_SECRET"
    secret  = google_secret_manager_secret.client_secret.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_DEVELOPER_TOKEN"
    secret  = google_secret_manager_secret.developer_token.secret_id
    version = "latest"
  }

}


resource "google_cloudfunctions_function" "google_ads_report_channel" {
  region                = var.region
  name                  = "vid-excl-google_ads_report_channel"
  description           = "Move the channel placement report from Google Ads to BigQuery."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.google_ads_report_channel.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.google_ads_report_channel
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.google_ads_account.name
  }

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS             = false
    GOOGLE_ADS_LOGIN_CUSTOMER_ID          = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT                  = var.project_id
    VID_EXCL_BIGQUERY_DATASET             = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
    VID_EXCL_YOUTUBE_CHANNEL_PUBSUB_TOPIC = google_pubsub_topic.youtube_channel.name
  }

  secret_environment_variables {
    key     = "GOOGLE_ADS_REFRESH_TOKEN"
    secret  = google_secret_manager_secret.oauth_refresh_token.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_ID"
    secret  = google_secret_manager_secret.client_id.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_SECRET"
    secret  = google_secret_manager_secret.client_secret.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_DEVELOPER_TOKEN"
    secret  = google_secret_manager_secret.developer_token.secret_id
    version = "latest"
  }

}


resource "google_cloudfunctions_function" "youtube_channel" {
  region                = var.region
  name                  = "vid-excl-youtube_channels"
  description           = "Pull the channel data from the YouTube API."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.youtube_channel.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_channel
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_channel.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT      = var.project_id
    VID_EXCL_BIGQUERY_DATASET = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  }
}


resource "google_cloudfunctions_function" "youtube_video" {
  region                = var.region
  name                  = "vid-excl-youtube_videos"
  description           = "Pull the video data from the YouTube API."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.youtube_video.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_video
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_video.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT      = var.project_id
    VID_EXCL_BIGQUERY_DATASET = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  }
}


resource "google_cloudfunctions_function" "youtube_thumbnails_dispatch" {
  region                = var.region
  name                  = "vid-excl-youtube_thumbnails_dispatch"
  description           = "Dispatch video IDs for thumbnail processing."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.youtube_thumbnails_dispatch.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_thumbnails_dispatch
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_video.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT                = var.project_id
    VID_EXCL_THUMBNAIL_PROCESSING_TOPIC = google_pubsub_topic.youtube_thumbnails_to_process.name
    VID_EXCL_BIGQUERY_DATASET           = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
    ENABLE_VISION_PROCESSING            = var.enable_vision_processing
  }
}


resource "google_cloudfunctions_function" "youtube_thumbnails_identify_objects" {
  region                = var.region
  name                  = "vid-excl-youtube_thumbnails_identify_objects"
  description           = "Identify objects and labels in a thumbnail."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.youtube_thumbnails_identify_objects.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_thumbnails_identify_objects
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_thumbnails_to_process.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT                           = var.project_id
    VID_EXCL_CROP_OBJECTS                          = var.crop_objects
    VID_EXCL_BIGQUERY_DATASET                      = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
    VID_EXCL_THUMBNAILS_TO_GENERATE_CROPOUTS_TOPIC = google_pubsub_topic.youtube_thumbnails_to_generate_cropouts.name
  }
}


resource "google_cloudfunctions_function" "youtube_thumbnails_generate_cropouts" {
  region                = var.region
  name                  = "vid-excl-youtube_thumbnails_generate_cropouts"
  description           = "Crop objects from video thumbnails."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.youtube_thumbnails_generate_cropouts.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 1024
  entry_point           = "main"
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.youtube_thumbnails_generate_cropouts
  ]

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.youtube_thumbnails_to_generate_cropouts.name
  }

  environment_variables = {
    GOOGLE_CLOUD_PROJECT           = var.project_id
    VID_EXCL_BIGQUERY_DATASET      = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
    VID_EXCL_THUMBNAIL_CROP_BUCKET = google_storage_bucket.thumbnail_cropouts.name
  }
}

resource "google_cloudfunctions_function" "google_ads_excluder" {
  region                = var.region
  name                  = "vid-excl-google_ads_excluder"
  description           = "Upload a list of videos/channels to exclude to a given account/exclusion list."
  runtime               = "python311"
  source_archive_bucket = google_storage_bucket.source_archive.name
  source_archive_object = google_storage_bucket_object.google_ads_excluder.name
  service_account_email = google_service_account.video_exclusion_toolbox.email
  build_service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.video_exclusion_toolbox.email}"
  timeout               = 540
  available_memory_mb   = 4096
  entry_point           = "main"
  trigger_http          = true
  depends_on = [
    resource.time_sleep.wait_60_seconds_after_role_assignment,
    resource.google_storage_bucket_object.google_ads_excluder
  ]

  environment_variables = {
    GOOGLE_ADS_USE_PROTO_PLUS    = false
    GOOGLE_ADS_LOGIN_CUSTOMER_ID = var.google_ads_login_customer_id
    GOOGLE_CLOUD_PROJECT         = var.project_id
    VID_EXCL_BIGQUERY_DATASET    = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  }

  secret_environment_variables {
    key     = "GOOGLE_ADS_REFRESH_TOKEN"
    secret  = google_secret_manager_secret.oauth_refresh_token.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_ID"
    secret  = google_secret_manager_secret.client_id.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_CLIENT_SECRET"
    secret  = google_secret_manager_secret.client_secret.secret_id
    version = "latest"
  }
  secret_environment_variables {
    key     = "GOOGLE_ADS_DEVELOPER_TOKEN"
    secret  = google_secret_manager_secret.developer_token.secret_id
    version = "latest"
  }
}