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


# This bucket is used to store the cloud functions for deployment.
# The project ID is used to make sure the name is globally unique
resource "google_storage_bucket" "thumbnail_cropouts_bucket" {
  name                        = "${var.project_id}-thumbnail-cropouts"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 5
    }
    action {
      type = "Delete"
    }
  }
}