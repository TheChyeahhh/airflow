# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest

import airflow.version
from airflow.models import Connection

from tests_common.test_utils.config import conf_vars

AZURE_CONN_ID = "wasb_dags_connection"
AZURE_CONTAINER_NAME = "my-airflow-dags-container"
AZURE_BLOB_PREFIX = "project1/dags"


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.mark.skipif(not airflow.version.version.strip().startswith("3"), reason="Airflow >=3.0.0 test")
class TestAzureBlobStorageDagBundle:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=AZURE_CONN_ID,
                conn_type="wasb",
            )
        )

    def test_view_url_generates_default_url(self):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix=AZURE_BLOB_PREFIX,
            container_name=AZURE_CONTAINER_NAME,
        )

        url: str = bundle.view_url()
        assert "portal.azure.com" in url
        assert AZURE_CONTAINER_NAME in url

    def test_view_url_template_generates_default_url(self):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix=AZURE_BLOB_PREFIX,
            container_name=AZURE_CONTAINER_NAME,
        )

        url: str = bundle.view_url_template()
        assert "portal.azure.com" in url
        assert AZURE_CONTAINER_NAME in url

    def test_supports_versioning(self):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix=AZURE_BLOB_PREFIX,
            container_name=AZURE_CONTAINER_NAME,
        )
        assert AzureBlobStorageDagBundle.supports_versioning is False

        # set version, it's not supported
        bundle.version = "test_version"

        with pytest.raises(Exception, match="Refreshing a specific version is not supported"):
            bundle.refresh()
        with pytest.raises(Exception, match="Azure Blob Storage URL with version is not supported"):
            bundle.view_url("test_version")

    def test_local_dags_path_is_not_a_directory(self, bundle_temp_dir):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        bundle_name = "test"
        file_path = bundle_temp_dir / bundle_name
        file_path.touch()

        bundle = AzureBlobStorageDagBundle(
            name=bundle_name,
            wasb_conn_id=AZURE_CONN_ID,
            prefix="project1_dags",
            container_name="airflow_dags",
        )
        with pytest.raises(Exception, match=f"Local DAGs path: {file_path} is not a directory."):
            bundle.initialize()

    def test_correct_bundle_path_used(self):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix="project1_dags",
            container_name="airflow_dags",
        )
        assert str(bundle.base_dir) == str(bundle.wasb_dags_dir)

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.AzureBlobStorageDagBundle.wasb_hook",
        new_callable=PropertyMock,
    )
    def test_container_and_prefix_validated(self, mock_wasb_hook_property, bundle_temp_dir):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        mock_hook = MagicMock()
        mock_wasb_hook_property.return_value = mock_hook

        # Container doesn't exist - get_blobs_list raises exception
        mock_hook.get_blobs_list.side_effect = Exception("Container not found")

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix="project1_dags",
            container_name="non-existing-container",
        )
        with pytest.raises(
            Exception,
            match="Azure Blob Storage container 'non-existing-container' does not exist",
        ):
            bundle.initialize()

        mock_hook.get_blobs_list.side_effect = None
        mock_hook.get_blobs_list.return_value = []
        mock_hook.check_for_prefix.return_value = False

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix="non-existing-prefix",
            container_name=AZURE_CONTAINER_NAME,
        )
        with pytest.raises(
            Exception,
            match=f"Azure Blob Storage prefix 'wasb://{AZURE_CONTAINER_NAME}/non-existing-prefix' "
            f"does not exist.",
        ):
            bundle.initialize()

        mock_hook.check_for_prefix.return_value = True
        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix=AZURE_BLOB_PREFIX,
            container_name=AZURE_CONTAINER_NAME,
        )
        bundle._log.debug = MagicMock()
        # initialize succeeds with correct prefix and container
        bundle.initialize()
        mock_hook.get_blobs_list.assert_called_once_with(
            container_name=AZURE_CONTAINER_NAME,
            prefix=AZURE_BLOB_PREFIX,
            delimiter="/",
        )

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.AzureBlobStorageDagBundle.wasb_hook",
        new_callable=PropertyMock,
    )
    def test_refresh(self, mock_wasb_hook_property, bundle_temp_dir):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        mock_hook = MagicMock()
        mock_wasb_hook_property.return_value = mock_hook
        mock_hook.get_blobs_list.return_value = []
        mock_hook.check_for_prefix.return_value = True
        mock_hook.get_blobs_list_recursive.return_value = []

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            prefix=AZURE_BLOB_PREFIX,
            container_name=AZURE_CONTAINER_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading DAGs from wasb://%s/%s to %s",
            AZURE_CONTAINER_NAME,
            AZURE_BLOB_PREFIX,
            bundle.wasb_dags_dir,
        )

        bundle.initialize()
        assert bundle._log.debug.call_count == 1
        assert bundle._log.debug.call_args_list == [download_log_call]
        assert mock_hook.get_blobs_list_recursive.call_count == 1
        assert mock_hook.get_blobs_list_recursive.call_args_list == [
            call(container_name=AZURE_CONTAINER_NAME, prefix=AZURE_BLOB_PREFIX)
        ]

        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
        assert mock_hook.get_blobs_list_recursive.call_count == 2
        assert mock_hook.get_blobs_list_recursive.call_args_list == [
            call(container_name=AZURE_CONTAINER_NAME, prefix=AZURE_BLOB_PREFIX),
            call(container_name=AZURE_CONTAINER_NAME, prefix=AZURE_BLOB_PREFIX),
        ]

    @patch(
        "airflow.providers.microsoft.azure.bundles.wasb.AzureBlobStorageDagBundle.wasb_hook",
        new_callable=PropertyMock,
    )
    def test_refresh_without_prefix(self, mock_wasb_hook_property, bundle_temp_dir):
        from airflow.providers.microsoft.azure.bundles.wasb import AzureBlobStorageDagBundle

        mock_hook = MagicMock()
        mock_wasb_hook_property.return_value = mock_hook
        mock_hook.get_blobs_list.return_value = []
        mock_hook.get_blobs_list_recursive.return_value = []

        bundle = AzureBlobStorageDagBundle(
            name="test",
            wasb_conn_id=AZURE_CONN_ID,
            container_name=AZURE_CONTAINER_NAME,
        )
        bundle._log.debug = MagicMock()
        download_log_call = call(
            "Downloading DAGs from wasb://%s/%s to %s",
            AZURE_CONTAINER_NAME,
            "",
            bundle.wasb_dags_dir,
        )

        assert bundle.prefix == ""
        bundle.initialize()
        assert bundle._log.debug.call_count == 1
        assert bundle._log.debug.call_args_list == [download_log_call]
        assert mock_hook.get_blobs_list_recursive.call_count == 1
        assert mock_hook.get_blobs_list_recursive.call_args_list == [
            call(container_name=AZURE_CONTAINER_NAME, prefix=None)
        ]

        bundle.refresh()
        assert bundle._log.debug.call_count == 2
        assert bundle._log.debug.call_args_list == [download_log_call, download_log_call]
        assert mock_hook.get_blobs_list_recursive.call_count == 2
        assert mock_hook.get_blobs_list_recursive.call_args_list == [
            call(container_name=AZURE_CONTAINER_NAME, prefix=None),
            call(container_name=AZURE_CONTAINER_NAME, prefix=None),
        ]
