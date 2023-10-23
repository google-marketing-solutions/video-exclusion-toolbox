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

resource "google_secret_manager_secret" "oauth_refresh_token" {
  secret_id = "vid-excl-oauth-refresh-token"
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret" "client_id" {
  secret_id = "vid-excl-client-id"
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret" "client_secret" {
  secret_id = "vid-excl-client-secret"
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret" "developer_token" {
  secret_id = "vid-excl-developer-token"
  replication {
    auto {}
  }
}
