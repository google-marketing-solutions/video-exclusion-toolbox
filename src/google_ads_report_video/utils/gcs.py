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
"""Utilities for working with Google Cloud Storage."""
from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client
import pandas as pd


def upload_blob_from_df(df: pd.DataFrame, bucket: str, blob_name: str) -> Blob:
  """Uploads a Pandas DataFrame to a Google Clous Storage bucket.

  Args:
      df: The Pandas dataframe to upload.
      bucket (str): Google Cloud Storage bucket.
      blob_name (str): Google Cloud Storage blob name.

  Returns:
      Newly created Google Cloud Storage file blob.
  """
  return upload_blob_from_string(
      blob_string=df.to_csv(index=False), blob_name=blob_name, bucket=bucket
  )


def upload_blob_from_string(
    bucket: str,
    blob_string: str,
    blob_name: str,
    content_type: str = 'text/csv',
) -> Blob:
  """Uploads a file to Google Cloud Storage.

  Args:
      bucket (str): Google Cloud Storage bucket.
      blob_string (str): The content of the blob.
      blob_name (str): Google Cloud Storage blob name.
      content_type (optional str): The content type of the string, e.g.
        text/csv.

  Returns:
      Newly created Google Cloud Storage file blob.
  """
  blob = create_blob(bucket, blob_name)
  blob.upload_from_string(blob_string, content_type=content_type)
  return blob


def create_blob(bucket_name: str, blob_name: str) -> Blob:
  """Creates a blob on Google Cloud Storage.

  Args:
      bucket_name (str): Google Cloud Storage bucket.
      blob_name (str): Google Cloud Storage blob name.

  Returns:
        Google Cloud Storage file blob.
  """
  client = Client()
  bucket = client.bucket(bucket_name)
  blob = bucket.blob(blob_name)
  return blob
