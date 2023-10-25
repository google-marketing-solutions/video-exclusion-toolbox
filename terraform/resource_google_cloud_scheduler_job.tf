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

locals {
  scheduler_body = <<EOF
    {
        "sheet_id": "${var.config_sheet_id}"
    }
    EOF
}

resource "google_cloud_scheduler_job" "video_exclusion_toolbox_run_process" {
  name             = "video_exclusion_toolbox"
  description      = "Run the Video Exclusion Toolbox"
  schedule         = "0 * * * *"
  time_zone        = "Etc/UTC"
  attempt_deadline = "320s"
  region           = var.region

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions_function.google_ads_accounts.https_trigger_url
    body        = base64encode(local.scheduler_body)
    headers = {
      "Content-Type" = "application/json"
    }
    oidc_token {
      service_account_email = google_service_account.video_exclusion_toolbox.email
    }
  }
}