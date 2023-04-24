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
data "archive_file" "youtube_thumbnails_zip" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_source.zip"
  source_dir  = "../src/youtube_thumbnails/"
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
resource "google_storage_bucket_object" "youtube_thumbnails" {
  name       = "youtube_thumbnails_${data.archive_file.youtube_thumbnails_zip.output_md5}.zip"
  bucket     = google_storage_bucket.function_bucket.name
  source     = data.archive_file.youtube_thumbnails_zip.output_path
  depends_on = [data.archive_file.youtube_thumbnails_zip]
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


resource "google_cloudfunctions_function" "youtube_thumbnails_function" {
  region                = var.region
  name                  = "vid-excl-youtube_thumbnails"
  description           = "Pull the YouTube video thumbnails and crop them."
  runtime               = "python310"
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.youtube_thumbnails.name
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
    VID_EXCL_THUMBNAIL_CROP_BUCKET      = google_storage_bucket.video_exclusion_data_bucket.name
    SEND_CROPOUTS_TO_PUBSUB             = google_storage_bucket.video_exclusion_data_bucket.name
    CROPPED_IMAGES_PUBSUB_TOPIC         = google_storage_bucket.video_exclusion_data_bucket.name
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