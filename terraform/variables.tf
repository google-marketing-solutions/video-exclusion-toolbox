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

variable "google_ads_client_version" {
  type        = string
  description = "The version of the Google Ads client library to use."
  default     = "v25"
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
  default     = "video_exclusion_toolbox"
}

variable "looker_studio_template" {
  type        = string
  description = "The ID of the template Looker Studio dashboard"
  default     = "2194043e-84bb-432b-b3e6-0553369745be"
}

variable "crop_objects" {
  type        = bool
  description = "Switch whether to crop the objects detected in a thumbnail."
  default     = true
}

variable "enable_vision_processing" {
  type        = bool
  description = "Switch whether to use vision processing on thumbnails."
  default     = false
}

variable "keyword_min_retention_ratio" {
  type        = number
  description = "Keyword matcher circuit breaker: the minimum fraction of the previous run's detections that the shared keywords must still yield before the run is allowed to commit."
  default     = 0.9

  validation {
    condition     = var.keyword_min_retention_ratio >= 0.0 && var.keyword_min_retention_ratio <= 1.0
    error_message = "keyword_min_retention_ratio must be between 0.0 and 1.0."
  }
}

variable "keyword_min_keyword_retention_ratio" {
  type        = number
  description = "Keyword matcher circuit breaker: the minimum fraction of the previous keyword set that must still be present before the run is allowed to commit without an explicit acknowledgement."
  default     = 0.9

  validation {
    condition     = var.keyword_min_keyword_retention_ratio >= 0.0 && var.keyword_min_keyword_retention_ratio <= 1.0
    error_message = "keyword_min_keyword_retention_ratio must be between 0.0 and 1.0."
  }
}

variable "enable_gemini_video_evaluation" {
  type        = bool
  description = "Switch whether to enable the Cloud Scheduler tick for the Multimodal Gemini Video & Thumbnail Evaluator."
  default     = true
}

variable "gemini_video_evaluator_model" {
  type        = string
  description = "The Gemini model used by vet-gemini-video-evaluator for Agent Platform Batch Inference."
  default     = "gemini-3.8-flash"
}

variable "gemini_video_evaluator_location" {
  type        = string
  description = "The Agent Platform location for vet-gemini-video-evaluator Batch Inference jobs."
  default     = "global"
}

variable "gemini_video_evaluator_max_videos" {
  type        = number
  description = "Upper limit on the number of videos sent to Gemini in one Agent Platform Batch Inference job. The max_videos_per_batch setting in the Config Sheet can lower it, but not raise it."
  default     = 10000

  validation {
    condition     = var.gemini_video_evaluator_max_videos >= 1 && floor(var.gemini_video_evaluator_max_videos) == var.gemini_video_evaluator_max_videos
    error_message = "gemini_video_evaluator_max_videos must be a whole number of at least 1."
  }
}


