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

# PUB/SUB ----------------------------------------------------------------------
resource "google_pubsub_topic" "google_ads_account" {
  name                       = "vid-excl-google-ads-account"
  message_retention_duration = "604800s"
}

resource "google_pubsub_topic" "youtube_video" {
  name                       = "vid-excl-youtube-video"
  message_retention_duration = "604800s"
}

resource "google_pubsub_topic" "youtube_channel" {
  name                       = "vid-excl-youtube-channel"
  message_retention_duration = "604800s"
}

resource "google_pubsub_topic" "youtube_thumbnails_to_process" {
  name                       = "vid-excl-youtube-thumbnails-to-process"
  message_retention_duration = "604800s"
}

resource "google_pubsub_topic" "youtube_thumbnails_to_generate_cropouts" {
  name                       = "vid-excl-youtube-thumbnails-to-generate-cropouts"
  message_retention_duration = "604800s"
}
