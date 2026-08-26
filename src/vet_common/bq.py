# Copyright 2026 Google LLC
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

"""BigQuery ingestion and deduplication utilities."""

import datetime
import io
import logging
from typing import Any, Optional, Union
import uuid

from google.cloud import bigquery


def write_ndjson_to_bq(
    client: bigquery.Client,
    ndjson_buffer: io.BytesIO,
    destination: str,
    write_disposition: str = 'WRITE_APPEND',
    schema: Optional[list[bigquery.SchemaField]] = None,
    logger: Optional[logging.Logger] = None,
) -> int:
  """Writes an in-memory NDJSON buffer to BigQuery using a LoadJob.

  Args:
      client: The BigQuery client.
      ndjson_buffer: The BytesIO buffer containing newline-delimited JSON.
      destination: Fully qualified table destination (project.dataset.table).
      write_disposition: BigQuery write disposition (defaults to
        'WRITE_APPEND').
      schema: Optional explicit BigQuery schema field definitions.
      logger: Optional logger for emitting progress information.

  Returns:
      Total number of rows loaded.
  """
  log = logger or logging.getLogger(__name__)
  ndjson_buffer.seek(0)

  job_config = bigquery.LoadJobConfig(
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition=write_disposition,
  )
  if schema:
    job_config.schema = schema

  job = client.load_table_from_file(
      ndjson_buffer, destination, job_config=job_config
  )
  job.result()
  output_rows = job.output_rows or 0
  log.info('Successfully wrote %d records to %s.', output_rows, destination)
  return output_rows




def upsert_ndjson_to_bq(
    client: bigquery.Client,
    ndjson_buffer: io.BytesIO,
    project_id: str,
    dataset_id: str,
    table_name: str,
    key_columns: list[str],
    partition_date: Union[datetime.date, str],
    timestamp_column: str = 'datetime_updated',
    schema: Optional[list[bigquery.SchemaField]] = None,
    log_prefix: str = '',
    logger: Optional[logging.Logger] = None,
) -> int:
  """Upserts an NDJSON buffer into a partitioned BigQuery table using a transient staging table and partition-scoped MERGE.

  Args:
      client: The BigQuery client.
      ndjson_buffer: In-memory io.BytesIO buffer containing newline-delimited
        JSON records.
      project_id: The GCP project ID.
      dataset_id: The BigQuery dataset ID.
      table_name: The target table name.
      key_columns: List of column names forming the unique composite key (e.g.
        ['customer_id', 'video_id']).
      partition_date: The date partition for scoping the MERGE statement.
      timestamp_column: The partition timestamp column name (defaults to
        'datetime_updated').
      schema: Optional explicit BigQuery schema field definitions. If None, it
        is retrieved from the target table.
      log_prefix: Optional string prefix to prepend to log messages (e.g.
        '[1234567890] ').
      logger: Optional logger for progress emitting.

  Returns:
      Total number of rows loaded and merged.
  """
  log = logger or logging.getLogger(__name__)
  ndjson_buffer.seek(0)

  target_table_id = f'{project_id}.{dataset_id}.{table_name}'
  if schema is None:
    target_table = client.get_table(target_table_id)
    schema = target_table.schema

  unique_suffix = uuid.uuid4().hex[:8]
  staging_table_id = (
      f'{project_id}.{dataset_id}._stage_{table_name.lower()}_{unique_suffix}'
  )

  staging_table = bigquery.Table(staging_table_id, schema=schema)
  staging_table.expires = datetime.datetime.now(
      datetime.timezone.utc
  ) + datetime.timedelta(hours=1)
  client.create_table(staging_table)
  log.debug(
      '%sCreated transient staging table %s', log_prefix, staging_table_id
  )

  try:
    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_TRUNCATE',
        schema=schema,
    )
    load_job = client.load_table_from_file(
        ndjson_buffer, staging_table_id, job_config=load_job_config
    )
    load_job.result()

    output_rows = load_job.output_rows or 0
    if output_rows == 0:
      log.info(
          '%sNo records loaded into staging table %s; skipping MERGE.',
          log_prefix,
          staging_table_id,
      )
      return 0

    date_str = (
        partition_date.isoformat()
        if hasattr(partition_date, 'isoformat')
        else str(partition_date)
    )
    match_predicates = ' AND '.join(
        [f'target.{col} = source.{col}' for col in key_columns]
    )
    match_predicates += (
        f' AND TIMESTAMP_TRUNC(target.{timestamp_column}, DAY) ='
        f' TIMESTAMP("{date_str}")'
    )

    update_cols = [
        field.name for field in schema if field.name not in key_columns
    ]
    update_assignments = ', '.join(
        [f'{col} = source.{col}' for col in update_cols]
    )

    merge_sql = f"""
      MERGE `{target_table_id}` AS target
      USING (
        SELECT * EXCEPT(row_num)
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY {', '.join(key_columns)}
              ORDER BY {timestamp_column} DESC
            ) AS row_num
          FROM `{staging_table_id}`
        )
        WHERE row_num = 1
      ) AS source
      ON {match_predicates}
      WHEN MATCHED THEN
        UPDATE SET {update_assignments}
      WHEN NOT MATCHED THEN
        INSERT ROW;
    """
    log.debug('%sExecuting partition MERGE query: %s', log_prefix, merge_sql)
    merge_job = client.query(merge_sql)
    merge_job.result()

    num_dml_affected_rows = getattr(merge_job, 'num_dml_affected_rows', None)
    dml_stat_str = (
        f' ({num_dml_affected_rows} rows affected: inserted/updated)'
        if num_dml_affected_rows is not None
        else ''
    )

    log.info(
        '%sSuccessfully merged %d staging records into %s for partition %s%s.',
        log_prefix,
        output_rows,
        target_table_id,
        date_str,
        dml_stat_str,
    )
    return output_rows
  finally:
    client.delete_table(staging_table_id, not_found_ok=True)
    log.debug(
        '%sCleaned up transient staging table %s', log_prefix, staging_table_id
    )


def get_existing_partition_ids(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    id_column: str,
    partition_date: Union[datetime.date, str],
    timestamp_column: str = 'datetime_updated',
    logger: Optional[logging.Logger] = None,
) -> set[str]:
  """Queries BigQuery and returns a set of existing IDs for a given date partition.

  Args:
      client: The BigQuery client.
      project_id: The GCP project ID.
      dataset_id: The BigQuery dataset ID.
      table_name: The target table name.
      id_column: The column name holding the entity ID (e.g. 'video_id',
        'channel_id').
      partition_date: The date partition to check.
      timestamp_column: The timestamp column name (defaults to
        'datetime_updated').
      logger: Optional logger for emitting debug information.

  Returns:
      A set of string IDs already present in BigQuery for the given partition
      date.
  """
  log = logger or logging.getLogger(__name__)

  date_str = (
      partition_date.isoformat()
      if hasattr(partition_date, 'isoformat')
      else str(partition_date)
  )

  query = f"""
      SELECT DISTINCT {id_column} FROM `{project_id}.{dataset_id}.{table_name}`
      WHERE TIMESTAMP_TRUNC({timestamp_column}, DAY) = TIMESTAMP("{date_str}")
  """
  log.debug('Querying existing partition IDs: %s', query)

  query_job = client.query(query)
  results = query_job.result()
  return {str(row[id_column]) for row in results if row[id_column] is not None}
