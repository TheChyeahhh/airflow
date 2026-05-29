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

import os
import shutil
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class AzureBlobStorageDagBundle(BaseDagBundle):
    """
    Azure Blob Storage DAG bundle - exposes a container/prefix in Azure Blob Storage as a DAG bundle.

    This allows Airflow to load DAGs directly from an Azure Blob Storage container.

    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage.
        Defaults to WasbHook.default_conn_name.
    :param container_name: The name of the Azure Blob Storage container containing the DAG files.
    :param prefix: Optional subdirectory (blob prefix) within the container where the DAGs are stored.
        If empty, DAGs are assumed to be at the root of the container.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        wasb_conn_id: str = WasbHook.default_conn_name,
        container_name: str,
        prefix: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        # Local path where Azure Blob Storage DAGs are downloaded
        self.azure_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            container_name=self.container_name,
            prefix=self.prefix,
            wasb_conn_id=self.wasb_conn_id,
        )
        self._wasb_hook: WasbHook | None = None

    def _initialize(self):
        with self.lock():
            if not self.azure_dags_dir.exists():
                self._log.info("Creating local DAGs directory: %s", self.azure_dags_dir)
                os.makedirs(self.azure_dags_dir)

            if not self.azure_dags_dir.is_dir():
                raise NotADirectoryError(f"Local DAGs path: {self.azure_dags_dir} is not a directory.")

            # Check that the container exists
            try:
                self._get_wasb_hook().get_conn().get_container_client(self.container_name).get_container_properties()
            except Exception as e:
                raise AirflowException(
                    f"Azure Blob Storage container '{self.container_name}' does not exist or is not accessible."
                ) from e

            if self.prefix:
                # don't check when prefix is ""
                if not self._get_wasb_hook().check_for_prefix(
                    container_name=self.container_name, prefix=self.prefix, delimiter="/"
                ):
                    raise AirflowException(
                        f"Azure Blob Storage prefix '{self.container_name}/{self.prefix}' does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def wasb_hook(self) -> WasbHook | None:
        if self._wasb_hook is None:
            try:
                self._wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
            except AirflowException as e:
                self._log.warning("Could not create WasbHook for connection %s: %s", self.wasb_conn_id, e)
        return self._wasb_hook

    def _get_wasb_hook(self) -> WasbHook:
        hook = self.wasb_hook
        if hook is None:
            raise AirflowException(f"Failed to create WasbHook for connection '{self.wasb_conn_id}'")
        return hook

    def __repr__(self):
        return (
            f"<AzureBlobStorageDagBundle("
            f"name={self.name!r}, "
            f"container_name={self.container_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the DAG bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the DAG files."""
        return self.azure_dags_dir  # Path where DAGs are downloaded.

    def refresh(self) -> None:
        """Refresh the DAG bundle by re-downloading the DAGs from Azure Blob Storage."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._log.debug(
                "Downloading DAGs from azure://%s/%s to %s",
                self.container_name,
                self.prefix,
                self.azure_dags_dir,
            )
            self._sync_to_local_dir()

    def _sync_to_local_dir(self) -> None:
        """
        Download all blobs matching the prefix from the container to the local directory.
        """
        blobs = self._get_wasb_hook().get_blobs_list_recursive(
            container_name=self.container_name,
            prefix=self.prefix,
        )

        if not blobs:
            self._log.warning(
                "No blobs found in container %s with prefix %s",
                self.container_name,
                self.prefix,
            )
            # Clear stale files if the source is empty
            if self.azure_dags_dir.exists():
                shutil.rmtree(self.azure_dags_dir)
                os.makedirs(self.azure_dags_dir)
            return

        # Track which files we've seen so we can delete stale ones
        seen_files: set[str] = set()

        for blob_name in blobs:
            # Calculate relative path by removing the prefix
            if self.prefix:
                # Ensure the blob name starts with the prefix
                if not blob_name.startswith(self.prefix):
                    continue
                relative_path = blob_name[len(self.prefix):].lstrip("/")
            else:
                relative_path = blob_name

            if not relative_path:
                continue

            local_path = self.azure_dags_dir / relative_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            self._log.debug(
                "Downloading blob %s to %s",
                blob_name,
                local_path,
            )
            try:
                self._get_wasb_hook().get_file(
                    file_path=str(local_path),
                    container_name=self.container_name,
                    blob_name=blob_name,
                )
                seen_files.add(local_path.name)
            except Exception as e:
                self._log.error(
                    "Failed to download blob %s: %s",
                    blob_name,
                    e,
                )

        # Delete stale files that no longer exist in the container
        if self.azure_dags_dir.exists():
            for local_path in self.azure_dags_dir.rglob("*"):
                if local_path.is_file() and local_path.name not in seen_files:
                    self._log.debug("Removing stale file: %s", local_path)
                    local_path.unlink()
            # Remove empty directories
            for local_path in sorted(self.azure_dags_dir.rglob("*"), key=lambda p: str(p), reverse=True):
                if local_path.is_dir() and not any(local_path.iterdir()):
                    local_path.rmdir()

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the DAGs in Azure Blob Storage. Currently, versioning is not supported.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the DAGs in Azure Blob Storage."""
        if self.version:
            raise AirflowException("Azure Blob Storage URL with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            return self._view_url_template
        # https://portal.azure.com/#browse/storageaccount/<account>/container/<container>/<prefix>
        url = (
            f"https://portal.azure.com/#browse/storageaccount/"
            f"{self.container_name}"
        )
        if self.prefix:
            url += f"/{self.prefix}"

        return url