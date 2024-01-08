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

resource "google_storage_bucket_object" "google_ads_accounts" {
  name       = "google_ads_accounts_${data.archive_file.google_ads_accounts.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.google_ads_accounts.output_path
  depends_on = [data.archive_file.google_ads_accounts]
}
resource "google_storage_bucket_object" "google_ads_exclusions" {
  name       = "google_ads_exclusions_${data.archive_file.google_ads_exclusions.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.google_ads_exclusions.output_path
  depends_on = [data.archive_file.google_ads_exclusions]
}
resource "google_storage_bucket_object" "google_ads_excluder" {
  name       = "google_ads_excluder_${data.archive_file.google_ads_excluder.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.google_ads_excluder.output_path
  depends_on = [data.archive_file.google_ads_excluder]
}
resource "google_storage_bucket_object" "google_ads_report_video" {
  name       = "google_ads_report_video${data.archive_file.google_ads_report_video.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.google_ads_report_video.output_path
  depends_on = [data.archive_file.google_ads_report_video]
}
resource "google_storage_bucket_object" "google_ads_report_channel" {
  name       = "google_ads_report_channel${data.archive_file.google_ads_report_channel.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.google_ads_report_channel.output_path
  depends_on = [data.archive_file.google_ads_report_channel]
}
resource "google_storage_bucket_object" "youtube_channel" {
  name       = "youtube_channel_${data.archive_file.youtube_channel.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.youtube_channel.output_path
  depends_on = [data.archive_file.youtube_channel]
}
resource "google_storage_bucket_object" "youtube_video" {
  name       = "youtube_video_${data.archive_file.youtube_video.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.youtube_video.output_path
  depends_on = [data.archive_file.youtube_video]
}
resource "google_storage_bucket_object" "youtube_thumbnails_dispatch" {
  name       = "youtube_thumbnails_dispatch_${data.archive_file.youtube_thumbnails_dispatch.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.youtube_thumbnails_dispatch.output_path
  depends_on = [data.archive_file.youtube_thumbnails_dispatch]
}
resource "google_storage_bucket_object" "youtube_thumbnails_identify_objects" {
  name       = "youtube_thumbnails_process_${data.archive_file.youtube_thumbnails_identify_objects.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.youtube_thumbnails_identify_objects.output_path
  depends_on = [data.archive_file.youtube_thumbnails_identify_objects]
}
resource "google_storage_bucket_object" "youtube_thumbnails_generate_cropouts" {
  name       = "youtube_thumbnails_generate_cropouts_${data.archive_file.youtube_thumbnails_generate_cropouts.output_md5}.zip"
  bucket     = google_storage_bucket.source_archive.name
  source     = data.archive_file.youtube_thumbnails_generate_cropouts.output_path
  depends_on = [data.archive_file.youtube_thumbnails_generate_cropouts]
}

resource "google_storage_bucket_object" "categories_lookup" {
  name          = "categories_lookup.csv"
  bucket        = google_storage_bucket.categories_lookup.name
  source        = "../src/categories_lookup.csv"
  content_type  = "text/plain"
}
