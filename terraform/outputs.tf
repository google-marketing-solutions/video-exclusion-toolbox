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

output "A_service_account_email" {
  value = <<-EOT



      _________________________________Step_1___________________________________
      ˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅
      Grant Sheet access to the service account below:
      
      --------------------------------------------------------------------------
      ${google_service_account.service_account.email}
      --------------------------------------------------------------------------

      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      ⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻
    EOT
}

output "B_run_cloud_scheduler" {
  value = <<-EOT



      _____________________________Step_2_(optional)____________________________
      ˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅

      After populating the Google Sheet you can trigger the cloud functions
      immediately by running the following command in the Cloud Shell:

      --------------------------------------------------------------------------
      gcloud scheduler jobs run video_exclusion_toolbox --location=${var.region} --project=${var.project_id}
      --------------------------------------------------------------------------

      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      ⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻
    EOT
}

output "C_looker_studio_url" {
  value = <<-EOT



      _________________________________Step_3___________________________________
      ˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅˅

      Click on the link below to create a new Looker Studio Dashboard, then
      follow the "Edit and Share" process:

      --------------------------------------------------------------------------
      https://lookerstudio.google.com/reporting/create?c.reportId=${var.looker_studio_template}&r.reportName=Video%20Exclusion%20Toolbox&ds.youtube_channel.datasourceName=YouTubeChannel&ds.youtube_channel.connector=bigQuery&ds.youtube_channel.type=TABLE&ds.youtube_channel.projectId=${var.project_id}&ds.youtube_channel.datasetId=${var.bq_dataset}&ds.youtube_channel.tableId=YouTubeChannel&ds.ads_and_channels.datasourceName=AdsAndChannels&ds.ads_and_channels.connector=bigQuery&ds.ads_and_channels.type=TABLE&ds.ads_and_channels.projectId=${var.project_id}&ds.ads_and_channels.datasetId=${var.bq_dataset}&ds.ads_and_channels.tableId=AdsAndYoutubeAndChannels&ds.ads_and_youtube.datasourceName=AdsAndYoutube&ds.ads_and_youtube.connector=bigQuery&ds.ads_and_youtube.type=TABLE&ds.ads_and_youtube.projectId=${var.project_id}&ds.ads_and_youtube.datasetId=${var.bq_dataset}&ds.ads_and_youtube.tableId=AdsAndYouTube
      --------------------------------------------------------------------------

      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      ⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻⎻
    EOT
}