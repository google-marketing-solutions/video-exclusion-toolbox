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

resource "google_storage_bucket" "source_archive" {
  name                        = "${var.project_id}-source-archive"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
  depends_on = [resource.google_project_iam_member.storage_object_admin]
}


resource "google_storage_bucket" "categories_lookup" {
  name                        = "${var.project_id}-categories-lookup"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}


resource "google_storage_bucket" "thumbnail_cropouts" {
  name                        = "${var.project_id}-thumbnail-cropouts"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}
