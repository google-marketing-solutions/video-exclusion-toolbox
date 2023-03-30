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

variable "project_id" {
  type        = string
  description = "The project ID to deploy the resources to"
}

variable "region" {
  type        = string
  description = "The region to deploy the resources to, e.g. europe-west2"
  default     = "europe-west2"
}

variable "oauth_refresh_token" {
  type        = string
  description = "The OAuth refresh token"
}

variable "google_cloud_client_id" {
  type        = string
  description = "The client ID from Google Cloud"
}

variable "google_cloud_client_secret" {
  type        = string
  description = "The client secret from Google Cloud"
}

variable "google_ads_developer_token" {
  type        = string
  description = "The Google Ads developer token"
}

variable "google_ads_login_customer_id" {
  type        = string
  description = "The Google Ads MCC customer ID with no dashes"
}

variable "config_sheet_id" {
  type        = string
  description = "The Google Sheeet ID containing the config"
}

variable "bq_dataset" {
  type        = string
  description = "The name of the BQ dataset"
  default     = "yt_ads_data"
}
