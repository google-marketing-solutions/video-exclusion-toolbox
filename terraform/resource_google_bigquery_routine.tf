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

resource "google_bigquery_routine" "identify_videos_with_keywords" {
  project             = "${var.project_id}"
  dataset_id      = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  routine_id      = "identify_videos_with_kewords"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.videos_with_matched_keywords
  ]
  definition_body = <<-EOS
    BEGIN
    DECLARE regex STRING;
    SET regex = (SELECT CONCAT('\\b(', STRING_AGG(DISTINCT keyword, '|' ORDER BY keyword),')[s]?\\b') FROM ${var.project_id}.${var.bq_dataset}.ExclusionKeywords);

    TRUNCATE TABLE ${var.project_id}.${var.bq_dataset}.VideosWithMatchedKeywords;

    INSERT INTO ${var.project_id}.${var.bq_dataset}.VideosWithMatchedKeywords (
      WITH
      keywords AS (
        SELECT
          keyword as keyword,
          CONCAT('\\b(', LOWER(keyword),')[s]?\\b') as keyword_regex
          FROM ${var.project_id}.${var.bq_dataset}.ExclusionKeywords
      ),
      videos AS (
        SELECT DISTINCT
          video_id,
          CONCAT('https://www.youtube.com/watch?v=', video_id) as video_url,
          channelId as channel_id,
          LOWER(title) as title,
          LOWER(description) as description,
          LOWER(ARRAY_TO_STRING(tags, ', ')) as tags
        from ${var.project_id}.${var.bq_dataset}.YouTubeVideo
        WHERE
          REGEXP_CONTAINS(LOWER(CONCAT(title, ' ', description, ' ', ARRAY_TO_STRING(tags, ', '))), regex)
      )
      SELECT
        v.video_id AS video_id,
        v.video_url AS video_url,
        v.channel_id AS channel_id,
        v.title AS title,
        v.description AS description,
        v.tags AS tags,
        STRING_AGG(DISTINCT CASE WHEN REGEXP_CONTAINS(v.title, k.keyword_regex) THEN k.keyword END, ', ') AS title_match,
        STRING_AGG(DISTINCT CASE WHEN REGEXP_CONTAINS(v.description, k.keyword_regex) THEN k.keyword END, ', ') AS description_match,
        STRING_AGG(DISTINCT CASE WHEN REGEXP_CONTAINS(v.tags, k.keyword_regex) THEN k.keyword END, ', ') AS tags_match
      FROM videos v
      CROSS JOIN keywords k
      WHERE (
        REGEXP_CONTAINS(v.title, k.keyword_regex) OR
        REGEXP_CONTAINS(v.description, k.keyword_regex) OR
        REGEXP_CONTAINS(v.tags, k.keyword_regex)
      )
      GROUP BY v.title, v.description, v.tags, v.video_id, v.video_url, v.channel_id
    );

    END

  EOS
}


resource "google_bigquery_routine" "identify_channels_with_keywords" {
  project             = "${var.project_id}"
  dataset_id      = google_bigquery_dataset.video_exclusion_toolbox.dataset_id
  routine_id      = "identify_channels_with_kewords"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  depends_on = [
    google_bigquery_dataset.video_exclusion_toolbox,
    google_bigquery_table.channels_with_matched_keywords
  ]
  definition_body = <<-EOS
    BEGIN
    DECLARE regex STRING;
    SET regex = (SELECT CONCAT('\\b(', STRING_AGG(DISTINCT keyword, '|' ORDER BY keyword),')[s]?\\b') FROM ${var.project_id}.${var.bq_dataset}.ExclusionKeywords);

    TRUNCATE TABLE ${var.project_id}.${var.bq_dataset}.ChannelsWithMatchedKeywords;

    INSERT INTO ${var.project_id}.${var.bq_dataset}.ChannelsWithMatchedKeywords (
      WITH
      keywords AS (
        SELECT
          keyword as keyword,
          CONCAT('\\b(', LOWER(keyword),')[s]?\\b') as keyword_regex
          FROM ${var.project_id}.${var.bq_dataset}.ExclusionKeywords
      ),
      channels AS (
        SELECT DISTINCT
          channel_id,
          CONCAT('https://www.youtube.com/channel/', channel_id) as channel_url,
          LOWER(title) as title
        from ${var.project_id}.${var.bq_dataset}.YouTubeChannel
        WHERE REGEXP_CONTAINS(LOWER(title), regex)
      )
      SELECT
        ch.channel_id AS channel_id,
        ch.channel_url AS channel_url,
        ch.title AS title,
        STRING_AGG(DISTINCT CASE WHEN REGEXP_CONTAINS(ch.title, k.keyword_regex) THEN k.keyword END, ', ') AS title_match,
      FROM channels ch
      CROSS JOIN keywords k
      WHERE REGEXP_CONTAINS(ch.title, k.keyword_regex)
      GROUP BY ch.title, ch.channel_id, ch.channel_url
    );

    END

  EOS
}