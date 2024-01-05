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

resource "google_bigquery_table" "youtube_category_lookup" {
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  project             = "${var.project_id}"
  deletion_protection = false
  depends_on = [
    resource.google_bigquery_dataset.video_exclusion_toolbox,
    resource.google_storage_bucket_object.categories_lookup
  ]
  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      "gs://${google_storage_bucket.categories_lookup.name}/categories_lookup.csv"
    ]
  }
  table_id = "YouTubeCategory"
}

resource "google_bigquery_table" "google_ads_report_video" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "GoogleAdsReportVideo"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/google_ads_report_video.json")
  time_partitioning {
    type  = DAY
    field = "datetime_updated"
  }
}

resource "google_bigquery_table" "google_ads_exclusions" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "GoogleAdsExclusions"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    source_uris = [
      "gs://${google_storage_bucket.video_exclusion_toolbox_data.name}/google_ads_exclusions/*.csv"
    ]
    schema = file("../bq_schemas/google_ads_exclusions.json")
    csv_options {
      quote             = ""
      skip_leading_rows = "1"
    }
  }
}

resource "google_bigquery_table" "google_ads_report_channel" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "GoogleAdsReportChannel"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    source_uris = [
      "gs://${google_storage_bucket.video_exclusion_toolbox_data.name}/google_ads_report_channel/*.csv"
    ]
    schema = file("../bq_schemas/google_ads_report_channel.json")
    csv_options {
      quote             = ""
      skip_leading_rows = "1"
    }
  }
}

resource "google_bigquery_table" "youtube_channel" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeChannel"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_channel.json")
}

resource "google_bigquery_table" "youtube_video" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeVideo"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_video.json")
}

resource "google_bigquery_table" "youtube_thubmnails" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeThumbnailsWithAnnotations"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_thumbnail_annotation.json")
}

resource "google_bigquery_table" "youtube_thubmnail_cropouts" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeThumbnailCropouts"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_thumbnail_cropout.json")
}

resource "google_bigquery_table" "exclusion_keywords" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "ExclusionKeywords"
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  external_data_configuration {
    autodetect    = false
    source_format = "GOOGLE_SHEETS"
    source_uris = [
      "https://docs.google.com/spreadsheets/d/${var.config_sheet_id}"
    ]
    schema = file("../bq_schemas/exclusion_keywords.json")
    google_sheets_options {
      range             = "exclusion_keywords!A:A"
      skip_leading_rows = "1"
    }
  }
}

resource "google_bigquery_table" "ads_and_youtube" {
  project             = "${var.project_id}"
  table_id            = "AdsAndYouTube"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.google_ads_report_video_aggregated,
    google_bigquery_table.youtube_video
  ]

  view {
    query          = <<-EOT
      SELECT
        Ads.first_seen,
        Ads.last_seen,
        customer_id,
        Ads.video_id,
        CONCAT('https://www.',youtube_video_url) as video_url,
        CONCAT('https://www.',youtube_channel_url) as channel_url,
        title,
        description,
        impressions,
        cost_micros,
        conversions,
        video_views,
        clicks,
        all_conversions_from_interactions_rate,
        publishedAt,
        channelId,
        categoryId,
        tags,
        defaultLanguage,
        duration,
        definition,
        licensedContent,
        ytContentRating,
        viewCount,
        likeCount,
        commentCount
      FROM
        `${var.project_id}.${var.bq_dataset}.GoogleAdsReportVideoAggregated` Ads
      LEFT JOIN
        `${var.project_id}.${var.bq_dataset}.YouTubeVideo` Video
      ON
        Ads.video_id = Video.video_id
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "ads_and_youtube_and_channels" {
  project             = "${var.project_id}"
  table_id            = "AdsAndYoutubeAndChannels"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.google_ads_report_video_aggregated,
    google_bigquery_table.youtube_video,
    google_bigquery_table.youtube_channel,
    google_bigquery_table.youtube_category_lookup
  ]
  view {
    query          = <<-EOT
      SELECT
        Ads.first_seen,
        Ads.last_seen,
        customer_id,
        Ads.video_id,
        CONCAT('https://www.',youtube_video_url) as video_url,
        CONCAT('https://www.',youtube_channel_url) as channel_url,
        Video.title,
        description,
        impressions,
        cost_micros,
        conversions,
        video_views,
        clicks,
        all_conversions_from_interactions_rate,
        publishedAt,
        channelId,
        categoryName,
        tags,
        defaultLanguage,
        duration,
        definition,
        licensedContent,
        ytContentRating,
        viewCount,
        likeCount,
        commentCount,
        Channel.country,
        Channel.title as channel_name,
        Channel.clean_topics as channel_topics,
        Channel.video_count as channel_video_count,
        Channel.subscriber_count as channel_subscribers
      FROM
        `${var.project_id}.${var.bq_dataset}.GoogleAdsReportVideoAggregated` Ads
      LEFT JOIN
        `${var.project_id}.${var.bq_dataset}.YouTubeVideo` Video
      ON
        Ads.video_id = Video.video_id
      LEFT JOIN
        `${var.project_id}.${var.bq_dataset}.YouTubeChannel` Channel
      ON
        Video.channelId = Channel.channel_id
      LEFT JOIN
        `${var.project_id}.${var.bq_dataset}.YouTubeCategory` Cat
      ON
        Video.categoryId = Cat.categoryId
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "google_ads_report_video_aggregated" {
  project             = "${var.project_id}"
  table_id            = "GoogleAdsReportVideoAggregated"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.google_ads_report_video
  ]
  view {
    query          = <<-EOT
      SELECT
        customer_id,
        video_id,
        youtube_video_name,
        youtube_video_url,
        youtube_channel_url,
        sum(impressions) impressions,
        sum(cost_micros) cost_micros,
        sum(conversions) conversions,
        sum(video_views) video_views,
        sum(clicks) clicks,
        sum(all_conversions_from_interactions_rate) all_conversions_from_interactions_rate,
        MIN(datetime_updated) first_seen,
        MAX(datetime_updated) last_seen
      FROM `${var.project_id}.${var.bq_dataset}.GoogleAdsReportVideo`
      group by 1,2,3,4,5
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "videos_to_exclude" {
  project             = "${var.project_id}"
  table_id            = "VideosToExclude"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.exclusion_keywords
  ]
  view {
    query          = <<-EOT
      SELECT
        DISTINCT video_id,
        title,
        description
      FROM
        `${var.project_id}.${var.bq_dataset}.YouTubeVideo`
      WHERE
        REGEXP_CONTAINS(LOWER(CONCAT(title, description, ARRAY_TO_STRING(tags, ' '))), (
          SELECT
            STRING_AGG(LOWER(keyword), '|')
          FROM
            `${var.project_id}.${var.bq_dataset}.ExclusionKeywords`));
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "channels_to_exclude" {
  project             = "${var.project_id}"
  table_id            = "ChannelsToExclude"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.exclusion_keywords
  ]
  view {
    query          = <<-EOT
      SELECT
        DISTINCT channel_id,
        title
      FROM
        `${var.project_id}.${var.bq_dataset}.YouTubeChannel`
      WHERE
        REGEXP_CONTAINS(LOWER((title)),(
          SELECT
            STRING_AGG(LOWER(keyword), '|')
          FROM
            `${var.project_id}.${var.bq_dataset}.ExclusionKeywords`));
    EOT
    use_legacy_sql = false
  }
}
