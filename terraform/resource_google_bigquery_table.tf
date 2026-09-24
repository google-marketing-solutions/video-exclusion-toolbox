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
  table_id            = "google_ads_report_channel"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/google_ads_report_channel.json")
  time_partitioning {
    type  = "DAY"
    field = "datetime_updated"
  }
}

resource "google_bigquery_table" "google_ads_report_channel_legacy_alias" {
  project             = "${var.project_id}"
  table_id            = "GoogleAdsReportChannel"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.google_ads_report_channel
  ]
  view {
    query          = <<-EOT
      SELECT * FROM `${var.project_id}.${var.bq_dataset}.google_ads_report_channel`
    EOT
    use_legacy_sql = false
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
  table_id            = "youtube_channel"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_channel.json")
}

resource "google_bigquery_table" "youtube_channel_legacy_alias" {
  project             = "${var.project_id}"
  table_id            = "YouTubeChannel"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.youtube_channel
  ]
  view {
    query          = <<-EOT
      SELECT * FROM `${var.project_id}.${var.bq_dataset}.youtube_channel`
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "youtube_video" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "youtube_video"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_video.json")
}

resource "google_bigquery_table" "youtube_video_legacy_alias" {
  project             = "${var.project_id}"
  table_id            = "YouTubeVideo"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.youtube_video
  ]
  view {
    query          = <<-EOT
      SELECT * FROM `${var.project_id}.${var.bq_dataset}.youtube_video`
    EOT
    use_legacy_sql = false
  }
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


########################## Keyword Compatibility Views #########################
# Compatibility views over the detection table exposing matched keywords in a
# comma-joined format per entity.
#
# They apply no policy filtering, deliberately: their name says "with matched
# keywords", not "to exclude", and that distinction is the whole point of
# splitting observation from policy. The ToExclude views below are where
# detection_policy is consulted.
resource "google_bigquery_table" "videos_with_matched_keywords" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "VideosWithMatchedKeywords"
  description         = "Videos with at least one exclusion keyword detected, in the legacy comma-joined shape. A view over the detection table."
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.detection
  ]
  view {
    query          = <<-EOT
      WITH detections AS (
        SELECT
          entity_id,
          STRING_AGG(DISTINCT IF(modality = 'title', label, NULL), ', ') AS title_match,
          STRING_AGG(DISTINCT IF(modality = 'description', label, NULL), ', ') AS description_match,
          STRING_AGG(DISTINCT IF(modality = 'tags', label, NULL), ', ') AS tags_match
        FROM `${var.project_id}.${var.bq_dataset}.detection`
        WHERE entity_type = 'video'
          AND detector = 'keyword_match'
          AND retracted_at IS NULL
        GROUP BY entity_id
      ),
      videos AS (
        SELECT
          video_id,
          channelId AS channel_id,
          title,
          description,
          ARRAY_TO_STRING(tags, ', ') AS tags
        FROM `${var.project_id}.${var.bq_dataset}.youtube_video`
        -- Deduplicate to the latest snapshot per video_id, matching the
        -- deduplication applied by the keyword matcher.
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY video_id ORDER BY datetime_updated DESC) = 1
      )
      SELECT
        d.entity_id AS video_id,
        CONCAT('https://www.youtube.com/watch?v=', d.entity_id) AS video_url,
        v.channel_id AS channel_id,
        v.title AS title,
        v.description AS description,
        v.tags AS tags,
        d.title_match AS title_match,
        d.description_match AS description_match,
        d.tags_match AS tags_match
      FROM detections AS d
      LEFT JOIN videos AS v ON v.video_id = d.entity_id
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "channels_with_matched_keywords" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "ChannelsWithMatchedKeywords"
  description         = "Channels with at least one exclusion keyword detected, in the legacy comma-joined shape. A view over the detection table."
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.detection
  ]
  view {
    query          = <<-EOT
      WITH detections AS (
        SELECT
          entity_id,
          STRING_AGG(DISTINCT IF(modality = 'title', label, NULL), ', ') AS title_match,
          STRING_AGG(DISTINCT IF(modality = 'description', label, NULL), ', ') AS description_match
        FROM `${var.project_id}.${var.bq_dataset}.detection`
        WHERE entity_type = 'channel'
          AND detector = 'keyword_match'
          AND retracted_at IS NULL
        GROUP BY entity_id
      ),
      channels AS (
        SELECT
          channel_id,
          custom_url,
          title,
          description
        FROM `${var.project_id}.${var.bq_dataset}.youtube_channel`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY channel_id ORDER BY datetime_updated DESC) = 1
      )
      SELECT
        d.entity_id AS channel_id,
        CONCAT('https://www.youtube.com/channel/', d.entity_id) AS channel_url,
        c.custom_url AS channel_custom_handle,
        c.title AS title,
        c.description AS description,
        d.title_match AS title_match,
        d.description_match AS description_match
      FROM detections AS d
      LEFT JOIN channels AS c ON c.channel_id = d.entity_id
    EOT
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "youtube_thumbnail_age_evaluation" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "youtube_thumbnail_age_evaluation"
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/youtube_thumbnail_age_evaluation.json")
}

############################### Detection Tables ###############################
# The observation store. One row per entity, modality and label; that tuple is
# the MERGE key. It is written by the keyword matcher and, in time, by other
# detectors, which is why the detector and taxonomy are columns rather than
# being implied by the table name.
resource "google_bigquery_table" "detection" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "detection"
  description         = "Detections made about YouTube entities, one row per entity, modality and label. Written by detectors; read by the policy views. Holds observations only - whether a detection leads to an exclusion is decided by detection_policy."
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/detection.json")
  # detector is placed early because each detector's MERGE touches only its own
  # rows, so this is the key that prunes. Physical layout is specified here
  # deliberately rather than left to default.
  clustering = ["entity_type", "detector", "taxonomy", "entity_id"]
}

# The policy store. Separating this from detection is what allows the meaning
# of a detection to change without recomputing it, and what keeps the
# ToExclude views declarative.
resource "google_bigquery_table" "detection_policy" {
  project             = "${var.project_id}"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  table_id            = "detection_policy"
  description         = "Decides which detections become exclusions. Rows are matched against detection on the non-NULL fields; among the matches the highest enabled priority wins."
  deletion_protection = true
  depends_on          = [google_bigquery_dataset.video_exclusion_toolbox]
  schema              = file("../bq_schemas/detection_policy.json")
}

locals {
  seed_detection_policy_sql = <<-EOT
    MERGE INTO `${var.project_id}.${google_bigquery_dataset.video_exclusion_toolbox.dataset_id}.${google_bigquery_table.detection_policy.table_id}` T
    USING (
      SELECT
        'first_party_keyword_exclude' AS policy_id,
        CAST(NULL AS STRING) AS entity_type,
        'first_party' AS source,
        'keyword_match' AS detector,
        'advertiser_blocklist.' AS taxonomy_prefix,
        CAST(NULL AS FLOAT64) AS min_score,
        'exclude' AS action,
        100 AS priority,
        TRUE AS enabled,
        'Default exclusion policy for first-party keyword matches' AS notes
    ) S
    ON T.policy_id = S.policy_id
    WHEN NOT MATCHED THEN
      INSERT (policy_id, entity_type, source, detector, taxonomy_prefix, min_score, action, priority, enabled, notes)
      VALUES (S.policy_id, S.entity_type, S.source, S.detector, S.taxonomy_prefix, S.min_score, S.action, S.priority, S.enabled, S.notes)
  EOT
}

resource "google_bigquery_job" "seed_detection_policy" {
  project  = var.project_id
  job_id   = "vet_seed_detection_policy_${substr(md5(local.seed_detection_policy_sql), 0, 8)}"
  location = google_bigquery_dataset.video_exclusion_toolbox.location

  query {
    query              = local.seed_detection_policy_sql
    use_legacy_sql     = false
    create_disposition = ""
    write_disposition  = ""
  }

  depends_on = [google_bigquery_table.detection_policy]
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
  table_id            = "exclusion_keywords"
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

resource "google_bigquery_table" "exclusion_keywords_legacy_alias" {
  project             = "${var.project_id}"
  table_id            = "ExclusionKeywords"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.exclusion_keywords
  ]
  view {
    query          = <<-EOT
      SELECT * FROM `${var.project_id}.${var.bq_dataset}.exclusion_keywords`
    EOT
    use_legacy_sql = false
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
        Video.description,
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

resource "google_bigquery_table" "google_ads_report_channel_aggregated" {
  project             = "${var.project_id}"
  table_id            = "GoogleAdsReportChannelAggregated"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.google_ads_report_channel
  ]
  view {
    query          = <<-EOT
      WITH latest_adgroup_placements AS (
        SELECT
          customer_id,
          campaign_id,
          ad_group_id,
          channel_id,
          youtube_channel_name,
          placement_target_url,
          impressions,
          cost_micros,
          conversions,
          video_views,
          clicks,
          datetime_updated
        FROM `${var.project_id}.${var.bq_dataset}.google_ads_report_channel`
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY customer_id, COALESCE(CAST(campaign_id AS STRING), ''), COALESCE(CAST(ad_group_id AS STRING), ''), channel_id 
          ORDER BY datetime_updated DESC
        ) = 1
      ),
      first_seen_timeline AS (
        SELECT
          customer_id,
          channel_id,
          MIN(datetime_updated) AS first_seen,
          MAX(datetime_updated) AS last_seen
        FROM `${var.project_id}.${var.bq_dataset}.google_ads_report_channel`
        GROUP BY 1, 2
      )
      SELECT
        l.customer_id,
        l.channel_id,
        ANY_VALUE(l.youtube_channel_name) AS youtube_channel_name,
        ANY_VALUE(l.placement_target_url) AS placement_target_url,
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
        ON l.customer_id = t.customer_id AND l.channel_id = t.channel_id
      GROUP BY 1, 2, t.first_seen, t.last_seen
    EOT
    use_legacy_sql = false
  }
}

############################ Policy-Filtered Views #############################
# These are the downstream excluder interface: src/google_ads_excluder/main.py
# reads them directly, so their output schemas are contractual and must not
# change.
#
# A detection becomes an exclusion only if an enabled policy matches it. Where
# several policies match, the highest priority decides, which is what allows a
# narrow 'ignore' to override a broad 'exclude'. A plain WHERE action =
# 'exclude' would have dropped that override on the floor.
resource "google_bigquery_table" "videos_to_exclude" {
  project             = "${var.project_id}"
  table_id            = "VideosToExclude"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.detection,
    google_bigquery_table.detection_policy
  ]
  view {
    query          = <<-EOT
      WITH policies AS (
        SELECT * FROM `${var.project_id}.${var.bq_dataset}.detection_policy`
        WHERE enabled
      ),
      -- One row per detection, carrying the action of the highest-priority
      -- policy that matches it. A detection matched by no policy is dropped
      -- by the inner join, which is the intended reading of "no policy".
      classified AS (
        SELECT
          d.entity_id AS entity_id,
          d.modality AS modality,
          d.label AS label,
          ARRAY_AGG(p.action ORDER BY p.priority DESC LIMIT 1)[OFFSET(0)] AS action
        FROM `${var.project_id}.${var.bq_dataset}.detection` AS d
        JOIN policies AS p
          ON (p.entity_type IS NULL OR p.entity_type = d.entity_type)
          AND (p.source IS NULL OR p.source = d.source)
          AND (p.detector IS NULL OR p.detector = d.detector)
          AND (p.taxonomy_prefix IS NULL OR STARTS_WITH(d.taxonomy, p.taxonomy_prefix))
          AND (p.min_score IS NULL OR d.score >= p.min_score)
        WHERE d.entity_type = 'video'
          AND d.retracted_at IS NULL
        GROUP BY d.entity_id, d.modality, d.label
      ),
      matched AS (
        SELECT
          entity_id,
          STRING_AGG(DISTINCT IF(modality = 'title', label, NULL), ', ') AS title_match,
          STRING_AGG(DISTINCT IF(modality = 'description', label, NULL), ', ') AS description_match,
          STRING_AGG(DISTINCT IF(modality = 'tags', label, NULL), ', ') AS tags_match
        FROM classified
        WHERE action = 'exclude'
        GROUP BY entity_id
      ),
      videos AS (
        SELECT
          video_id,
          title,
          description,
          ARRAY_TO_STRING(tags, ', ') AS tags,
          availability_status
        FROM `${var.project_id}.${var.bq_dataset}.youtube_video`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY video_id ORDER BY datetime_updated DESC) = 1
      )
      SELECT
        m.entity_id AS video_id,
        CONCAT('https://www.youtube.com/watch?v=', m.entity_id) AS video_url,
        v.title AS title,
        v.description AS description,
        v.tags AS tags,
        -- CONCAT returns NULL for a NULL match and ARRAY_TO_STRING skips
        -- NULLs, so a modality that matched nothing leaves no trace. This is
        -- the behaviour of the original expression, preserved.
        CONCAT(
          'Found: ',
          ARRAY_TO_STRING([
            CONCAT('[', m.title_match, '] in title'),
            CONCAT('[', m.description_match, '] in description'),
            CONCAT('[', m.tags_match, '] in tags')],
          ', ')
        ) AS reason
      FROM matched AS m
      LEFT JOIN videos AS v ON v.video_id = m.entity_id
      WHERE (v.availability_status IS NULL OR v.availability_status != 'DELETED')
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
    google_bigquery_table.detection,
    google_bigquery_table.detection_policy
  ]
  view {
    query          = <<-EOT
      WITH policies AS (
        SELECT * FROM `${var.project_id}.${var.bq_dataset}.detection_policy`
        WHERE enabled
      ),
      classified AS (
        SELECT
          d.entity_id AS entity_id,
          d.modality AS modality,
          d.label AS label,
          ARRAY_AGG(p.action ORDER BY p.priority DESC LIMIT 1)[OFFSET(0)] AS action
        FROM `${var.project_id}.${var.bq_dataset}.detection` AS d
        JOIN policies AS p
          ON (p.entity_type IS NULL OR p.entity_type = d.entity_type)
          AND (p.source IS NULL OR p.source = d.source)
          AND (p.detector IS NULL OR p.detector = d.detector)
          AND (p.taxonomy_prefix IS NULL OR STARTS_WITH(d.taxonomy, p.taxonomy_prefix))
          AND (p.min_score IS NULL OR d.score >= p.min_score)
        WHERE d.entity_type = 'channel'
          AND d.retracted_at IS NULL
        GROUP BY d.entity_id, d.modality, d.label
      ),
      matched AS (
        SELECT
          entity_id,
          STRING_AGG(DISTINCT IF(modality = 'title', label, NULL), ', ') AS title_match,
          STRING_AGG(DISTINCT IF(modality = 'description', label, NULL), ', ') AS description_match
        FROM classified
        WHERE action = 'exclude'
        GROUP BY entity_id
      ),
      channels AS (
        SELECT
          channel_id,
          title
        FROM `${var.project_id}.${var.bq_dataset}.youtube_channel`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY channel_id ORDER BY datetime_updated DESC) = 1
      )
      SELECT
        m.entity_id AS channel_id,
        CONCAT('https://www.youtube.com/channel/', m.entity_id) AS channel_url,
        c.title AS title,
        -- The column list is unchanged, and the reason text reports both title
        -- and description matches so that channels matched via description
        -- carry an accurate attribution reason.
        CONCAT(
          'Found: ',
          ARRAY_TO_STRING([
            CONCAT('[', m.title_match, '] in title'),
            CONCAT('[', m.description_match, '] in description')],
          ', ')
        ) AS reason
      FROM matched AS m
      LEFT JOIN channels AS c ON c.channel_id = m.entity_id
    EOT
    use_legacy_sql = false
  }
}


############################### Statistics Views ###############################
# Counts of how often each keyword fires, per modality. These read detection
# directly rather than re-splitting the comma-joined compatibility views: the
# detection rows are already one per keyword, so the SPLIT and UNNEST are not
# needed, avoiding fragile string splitting on comma-joined columns.
#
# As with the compatibility views, no policy filter is applied: the question
# these answer is "which keywords fire", not "which entities are excluded".
resource "google_bigquery_table" "video_keyword_statistics" {
  project             = "${var.project_id}"
  table_id            = "VideoKeywordStatistics"
  dataset_id          = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  deletion_protection = false
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.detection
  ]
  view {
    query          = <<-EOT
      SELECT
        label AS keyword,
        COUNT(*) AS total_matched_keywords,
        COUNTIF(modality = 'title') AS title_matched_keywords,
        COUNTIF(modality = 'description') AS description_matched_keywords,
        COUNTIF(modality = 'tags') AS tags_matched_keywords
      FROM `${var.project_id}.${var.bq_dataset}.detection`
      WHERE entity_type = 'video'
        AND detector = 'keyword_match'
        AND retracted_at IS NULL
      GROUP BY keyword
      ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC
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
    google_bigquery_table.detection
  ]
  view {
    query          = <<-EOT
      SELECT
        label AS keyword,
        COUNT(*) AS total_matched_keywords,
        COUNTIF(modality = 'title') AS title_matched_keywords,
        COUNTIF(modality = 'description') AS description_matched_keywords
      FROM `${var.project_id}.${var.bq_dataset}.detection`
      WHERE entity_type = 'channel'
        AND detector = 'keyword_match'
        AND retracted_at IS NULL
      GROUP BY keyword
      ORDER BY 2 DESC, 1
    EOT
    use_legacy_sql = false
  }
}

