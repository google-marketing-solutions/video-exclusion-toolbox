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
"""Utilities for loading dataframes to BigQuery."""

from google.cloud import bigquery
import pandas as pd


def load_to_bq_from_df(
    df: pd.DataFrame,
    table_id: str,
) -> None:
  """Uploads a Pandas DataFrame to a BigQuery table.

  Args:
      df: The Pandas dataframe to upload.
      table_id: The id of the BQ table, example:
        "your-project.your_dataset.your_table_name"
  """
  # Construct a BigQuery client object.
  client = bigquery.Client()
  schema = [
      bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
      bigquery.SchemaField("topic_categories", "STRING", mode="REPEATED"),
      bigquery.SchemaField("view_count", bigquery.enums.SqlTypeNames.INTEGER),
      bigquery.SchemaField("video_count", bigquery.enums.SqlTypeNames.INTEGER),
      bigquery.SchemaField(
          "subscriber_count", bigquery.enums.SqlTypeNames.INTEGER
      ),
      bigquery.SchemaField("title", "STRING"),
      bigquery.SchemaField("country", "STRING"),
      bigquery.SchemaField(
          "datetime_updated",
          bigquery.enums.SqlTypeNames.TIMESTAMP,
          mode="REQUIRED",
      ),
      bigquery.SchemaField("clean_topics", "STRING", mode="REPEATED"),
  ]

  job_config = bigquery.LoadJobConfig(
      schema=schema,
      write_disposition="WRITE_APPEND",
      source_format=bigquery.SourceFormat.PARQUET,
  )

  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
  job.result()
