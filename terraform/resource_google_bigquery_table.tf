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

################################# Native BQ tables #############################
resource "google_bigquery_table" "google_ads_report_video" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "google_ads_report_video"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/google_ads_report_video.json")
  time_partitioning {
    type  = "DAY"
    field = "datetime_updated"
  }
}

resource "google_bigquery_table" "google_ads_report_video_legacy_alias" {
  project             = "${var.project_id}"
  table_id            = "GoogleAdsReportVideo"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.google_ads_report_video
  ]
  view {
    query          = <<-EOT
      SELECT * FROM `${var.project_id}.${var.bq_dataset}.google_ads_report_video`
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "google_ads_report_channel" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "GoogleAdsReportChannel"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/google_ads_report_channel.json")
  time_partitioning {
    type  = "DAY"
    field = "datetime_updated"
  }
}

resource "google_bigquery_table" "google_ads_exclusions" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "GoogleAdsExclusions"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/google_ads_exclusions.json")
}

resource "google_bigquery_table" "youtube_channel" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeChannel"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_channel.json")
}

resource "google_bigquery_table" "youtube_video" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeVideo"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_video.json")
}

resource "google_bigquery_table" "youtube_thumbnails" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeThumbnailsWithAnnotations"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_thumbnail_annotation.json")
}

resource "google_bigquery_table" "youtube_thumbnail_cropouts" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "YouTubeThumbnailCropouts"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_thumbnail_cropout.json")
}


resource "google_bigquery_table" "videos_with_matched_keywords" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "VideosWithMatchedKeywords"
  description         = "This table is populated by the 'identify_videos_with_kewords' stored procedure."
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/videos_with_matched_keywords.json")
}


resource "google_bigquery_table" "channels_with_matched_keywords" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "ChannelsWithMatchedKeywords"
  description         = "This table is populated by the 'identify_channels_with_kewords' stored procedure."
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/channels_with_matched_keywords.json")
}

resource "google_bigquery_table" "youtube_thumbnail_age_evaluation" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "youtube_thumbnail_age_evaluation"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_thumbnail_age_evaluation.json")
}

############################## External BQ Tables ##############################
resource "google_bigquery_table" "youtube_category_lookup" {
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  project             = "${var.project_id}"
  deletion_protection = true
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

resource "google_bigquery_table" "exclusion_keywords" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "ExclusionKeywords"
  deletion_protection = true
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

#################################### Views #####################################
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
      WITH latest_adgroup_placements AS (
        SELECT
          customer_id,
          campaign_id,
          ad_group_id,
          video_id,
          youtube_video_name,
          youtube_video_url,
          youtube_channel_url,
          impressions,
          cost_micros,
          conversions,
          video_views,
          clicks,
          datetime_updated
        FROM `${var.project_id}.${var.bq_dataset}.google_ads_report_video`
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY customer_id, COALESCE(CAST(campaign_id AS STRING), ''), COALESCE(CAST(ad_group_id AS STRING), ''), video_id 
          ORDER BY datetime_updated DESC
        ) = 1
      ),
      first_seen_timeline AS (
        SELECT
          customer_id,
          video_id,
          MIN(datetime_updated) AS first_seen,
          MAX(datetime_updated) AS last_seen
        FROM `${var.project_id}.${var.bq_dataset}.google_ads_report_video`
        GROUP BY 1, 2
      )
      SELECT
        l.customer_id,
        l.video_id,
        ANY_VALUE(l.youtube_video_name) AS youtube_video_name,
        ANY_VALUE(l.youtube_video_url) AS youtube_video_url,
        ANY_VALUE(l.youtube_channel_url) AS youtube_channel_url,
        SUM(l.impressions) AS impressions,
        SUM(l.cost_micros) AS cost_micros,
        SUM(l.conversions) AS conversions,
        SUM(l.video_views) AS video_views,
        SUM(l.clicks) AS clicks,
        COALESCE(SAFE_DIVIDE(SUM(l.conversions), SUM(l.clicks)), 0.0) AS all_conversions_from_interactions_rate,
        t.first_seen,
        t.last_seen
      FROM latest_adgroup_placements l
      JOIN first_seen_timeline t
        ON l.customer_id = t.customer_id AND l.video_id = t.video_id
      GROUP BY 1, 2, t.first_seen, t.last_seen
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
    google_bigquery_table.videos_with_matched_keywords
  ]
  view {
    query          = <<-EOT
      SELECT
        DISTINCT video_id, video_url, title, description, tags,
        CONCAT(
          'Found: ',
          ARRAY_TO_STRING([
            CONCAT('[', title_match, '] in title'),
            CONCAT('[', description_match, '] in description'),
            CONCAT('[', tags_match, '] in tags')],
          ', ')
        ) as reason
      FROM ${var.project_id}.${var.bq_dataset}.VideosWithMatchedKeywords
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
    google_bigquery_table.channels_with_matched_keywords
  ]
  view {
    query          = <<-EOT
      SELECT
        DISTINCT channel_id, channel_url, title,
        CONCAT('Found: [', title_match, '] in title') as reason
      FROM ${var.project_id}.${var.bq_dataset}.ChannelsWithMatchedKeywords
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "video_keyword_statistics" {
  project             = "${var.project_id}"
  table_id            = "VideoKeywordStatistics"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.videos_with_matched_keywords
  ]
  view {
    query          = <<-EOT
      WITH exploded_table AS (
        SELECT
          SPLIT(title_match, ',') AS title_words,
          SPLIT(description_match, ',') AS description_words,
          SPLIT(tags_match, ',') AS tags_words
        FROM ${var.project_id}.${var.bq_dataset}.VideosWithMatchedKeywords
      ),
      title_keywords as (
        SELECT trim(words) as keyword FROM
        exploded_table,
        UNNEST(title_words) as words
      ),
      description_keywords as (
        SELECT trim(words) as keyword FROM
        exploded_table,
        UNNEST(description_words) as words
      ),
      tags_keywords as (
        SELECT trim(words) as keyword FROM
        exploded_table,
        UNNEST(tags_words) as words
      )
      SELECT
        keyword,
        COUNT(*) AS total_matched_keywords,
        SUM(CASE WHEN table = 'title_keywords' THEN 1 ELSE 0 END) AS title_matched_keywords,
        SUM(CASE WHEN table = 'description_keywords' THEN 1 ELSE 0 END) AS description_matched_keywords,
        SUM(CASE WHEN table = 'tags_keywords' THEN 1 ELSE 0 END) AS tags_matched_keywords
      FROM (
        SELECT keyword, 'title_keywords' AS table FROM title_keywords
        UNION ALL
        SELECT keyword, 'description_keywords' AS table FROM description_keywords
        UNION ALL
        SELECT keyword, 'tags_keywords' AS table FROM tags_keywords
      ) AS all_keywords
      GROUP BY keyword
      ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC;
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "channel_keyword_statistics" {
  project             = "${var.project_id}"
  table_id            = "ChannelKeywordStatistics"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
  ]
  view {
    query          = <<-EOT
      WITH exploded_table AS (
        SELECT
          SPLIT(title_match, ',') AS title_words
        FROM ${var.project_id}.${var.bq_dataset}.ChannelsWithMatchedKeywords
      ),
      title_keywords as (
        SELECT trim(words) as keyword FROM
        exploded_table,
        UNNEST(title_words) as words
      )
      SELECT
        keyword,
        COUNT(*) AS title_matched_keywords
      FROM title_keywords
      GROUP BY 1
      ORDER BY 2 DESC, 1;
    EOT
    use_legacy_sql = false
  }
}
