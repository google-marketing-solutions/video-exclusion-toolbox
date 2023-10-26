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

data "archive_file" "google_ads_accounts" {
  type        = "zip"
  output_path = ".temp/google_ads_accounts.zip"
  source_dir  = "../src/google_ads_accounts/"
}
data "archive_file" "google_ads_exclusions" {
  type        = "zip"
  output_path = ".temp/google_ads_exclusions.zip"
  source_dir  = "../src/google_ads_exclusions/"
}
data "archive_file" "google_ads_excluder" {
  type        = "zip"
  output_path = ".temp/google_ads_excluder.zip"
  source_dir  = "../src/google_ads_excluder/"
}
data "archive_file" "google_ads_report_video" {
  type        = "zip"
  output_path = ".temp/google_ads_report_video.zip"
  source_dir  = "../src/google_ads_report_video/"
}
data "archive_file" "google_ads_report_channel" {
  type        = "zip"
  output_path = ".temp/google_ads_report_channel.zip"
  source_dir  = "../src/google_ads_report_channel/"
}
data "archive_file" "youtube_channel" {
  type        = "zip"
  output_path = ".temp/youtube_channel.zip"
  source_dir  = "../src/youtube_channel/"
}
data "archive_file" "youtube_video" {
  type        = "zip"
  output_path = ".temp/youtube_video.zip"
  source_dir  = "../src/youtube_video/"
}
data "archive_file" "youtube_thumbnails_dispatch" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_dispatch.zip"
  source_dir  = "../src/youtube_thumbnails_dispatch/"
}
data "archive_file" "youtube_thumbnails_process" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_process.zip"
  source_dir  = "../src/youtube_thumbnails_process/"
}
data "archive_file" "youtube_thumbnails_generate_cropouts" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_generate_cropouts.zip"
  source_dir  = "../src/youtube_thumbnails_generate_cropouts/"
}
