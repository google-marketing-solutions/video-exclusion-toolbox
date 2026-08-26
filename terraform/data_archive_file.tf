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
  vet_common_sources = {
    for f in fileset("${path.module}/../src/vet_common", "*.py") :
    "vet_common/${f}" => file("${path.module}/../src/vet_common/${f}")
  }
}

data "archive_file" "gads_account_dispatcher" {
  type        = "zip"
  output_path = ".temp/gads_account_dispatcher.zip"
  source {
    content  = file("../src/gads_account_dispatcher/main.py")
    filename = "main.py"
  }
  source {
    content  = "${file("../src/common_requirements.txt")}\n${file("../src/gads_account_dispatcher/requirements.txt")}"
    filename = "requirements.txt"
  }
  dynamic "source" {
    for_each = local.vet_common_sources
    content {
      filename = source.key
      content  = source.value
    }
  }
  depends_on = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "google_ads_exclusions_fetcher" {
  type        = "zip"
  output_path = ".temp/google-ads-exclusions-fetcher.zip"
  source {
    content  = file("../src/google-ads-exclusions-fetcher/main.py")
    filename = "main.py"
  }
  source {
    content  = file("../src/google-ads-exclusions-fetcher/requirements.txt")
    filename = "requirements.txt"
  }
  depends_on = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "google_ads_excluder" {
  type        = "zip"
  output_path = ".temp/google_ads_excluder.zip"
  source_dir  = "../src/google_ads_excluder/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "gads_video_report_fetcher" {
  type        = "zip"
  output_path = ".temp/gads_video_report_fetcher.zip"
  source {
    content  = file("../src/gads_video_report_fetcher/main.py")
    filename = "main.py"
  }
  source {
    content  = "${file("../src/common_requirements.txt")}\n${file("../src/google_ads_requirements.txt")}\n${file("../src/gads_video_report_fetcher/requirements.txt")}"
    filename = "requirements.txt"
  }
  dynamic "source" {
    for_each = local.vet_common_sources
    content {
      filename = source.key
      content  = source.value
    }
  }
  depends_on = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "google_ads_report_channel" {
  type        = "zip"
  output_path = ".temp/google_ads_report_channel.zip"
  source_dir  = "../src/google_ads_report_channel/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "youtube_channel" {
  type        = "zip"
  output_path = ".temp/youtube_channel.zip"
  source_dir  = "../src/youtube_channel/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "youtube_video" {
  type        = "zip"
  output_path = ".temp/youtube_video.zip"
  source_dir  = "../src/youtube_video/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "youtube_thumbnails_dispatch" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_dispatch.zip"
  source_dir  = "../src/youtube_thumbnails_dispatch/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "youtube_thumbnails_identify_objects" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_identify_objects.zip"
  source_dir  = "../src/youtube_thumbnails_identify_objects/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "youtube_thumbnails_generate_cropouts" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_generate_cropouts.zip"
  source_dir  = "../src/youtube_thumbnails_generate_cropouts/"
  depends_on  = [resource.google_project_iam_member.storage_object_admin]
}

data "archive_file" "youtube_thumbnails_evaluate_age_dispatcher" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_evaluate_age_dispatcher.zip"
  source {
    content  = file("../src/youtube_thumbnails_evaluate_age_dispatcher/main.py")
    filename = "main.py"
  }
  source {
    content  = file("../src/youtube_thumbnails_evaluate_age_dispatcher/requirements.txt")
    filename = "requirements.txt"
  }
  depends_on = [resource.google_project_iam_member.storage_object_admin]
}
data "archive_file" "youtube_thumbnails_evaluate_age_processor" {
  type        = "zip"
  output_path = ".temp/youtube_thumbnails_evaluate_age_processor.zip"
  source {
    content  = file("../src/youtube_thumbnails_evaluate_age_processor/main.py")
    filename = "main.py"
  }
  source {
    content  = file("../src/youtube_thumbnails_evaluate_age_processor/requirements.txt")
    filename = "requirements.txt"
  }
  depends_on = [resource.google_project_iam_member.storage_object_admin]
}
