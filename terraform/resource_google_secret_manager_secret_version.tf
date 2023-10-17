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

resource "google_secret_manager_secret_version" "oauth_refresh_token" {
  secret = google_secret_manager_secret.oauth_refresh_token.id
  secret_data = var.oauth_refresh_token
}
resource "google_secret_manager_secret_version" "client_id" {
  secret = google_secret_manager_secret.client_id.id
  secret_data = var.google_cloud_client_id
}
resource "google_secret_manager_secret_version" "client_secret" {
  secret = google_secret_manager_secret.client_secret.id
  secret_data = var.google_cloud_client_secret
}
resource "google_secret_manager_secret_version" "developer_token" {
  secret = google_secret_manager_secret.developer_token.id
  secret_data = var.google_ads_developer_token
}