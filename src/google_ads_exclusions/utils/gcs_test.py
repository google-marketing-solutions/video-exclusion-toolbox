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
"""Tests for gcs."""
import unittest
from unittest import mock
import gcs
from google.cloud.storage.blob import Blob
import pandas as pd


class GcsTest(unittest.TestCase):

  def test_df_uploads_as_csv(self):
    with mock.patch.object(
        Blob, 'upload_from_string', autospec=True
    ) as patched:
      df = pd.DataFrame(data=[10, 20, 30], columns=['Numbers'])

      blob = gcs.upload_blob_from_df(
          df=df, bucket='bucket', blob_name='blob_name'
      )

      patched.assert_called_once_with(
          mock.ANY, 'Numbers\n10\n20\n30\n', 'text/csv'
      )
      self.assertIsInstance(blob, Blob)

if __name__ == '__main__':
  unittest.main()
