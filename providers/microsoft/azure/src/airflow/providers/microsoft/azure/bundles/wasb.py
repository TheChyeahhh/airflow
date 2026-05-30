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
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class AzureBlobStorageDagBundle(BaseDagBundle):
    """
    Azure Blob Storage DAG bundle - exposes a directory in Azure Blob Storage as a DAG bundle.

    This allows Airflow to load DAGs directly from an Azure Blob Storage container.

    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage.
        Defaults to WasbHook.default_conn_name.
    :param container_name: The name of the Azure Blob Storage container containing the DAG files.
    :param prefix: Optional subdirectory (blob prefix) within the container where the DAGs are stored.
        If empty, DAGs are assumed to be at the root of the container (Optional).
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
        self.wasb_dags_dir: Path = self.base_dir

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
            if not self.wasb_dags_dir.exists():
                self._log.info("Creating local DAGs directory: %s", self.wasb_dags_dir)
                os.makedirs(self.wasb_dags_dir)

            if not self.wasb_dags_dir.is_dir():
                raise AirflowException(f"Local DAGs path: {self.wasb_dags_dir} is not a directory.")

            # Validate that the container exists by attempting to list blobs
            try:
                self.wasb_hook.get_blobs_list(
                    container_name=self.container_name,
                    prefix=self.prefix if self.prefix else None,
                    delimiter="/",
                )
            except Exception as e:
                raise AirflowException(
                    f"Azure Blob Storage container '{self.container_name}' does not exist "
                    f"or is not accessible: {e}"
                )

            if self.prefix:
                if not self.wasb_hook.check_for_prefix(
                    container_name=self.container_name, prefix=self.prefix
                ):
                    raise AirflowException(
                        f"Azure Blob Storage prefix 'wasb://{self.container_name}/{self.prefix}' "
                        f"does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def wasb_hook(self):
        if self._wasb_hook is None:
            try:
                self._wasb_hook: WasbHook = WasbHook(wasb_conn_id=self.wasb_conn_id)
            except AirflowException as e:
                self._log.warning(
                    "Could not create WasbHook for connection %s: %s", self.wasb_conn_id, e
                )
        return self._wasb_hook

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
        return self.wasb_dags_dir  # Path where DAGs are downloaded.

    def refresh(self) -> None:
        """Refresh the DAG bundle by re-downloading the DAGs from Azure Blob Storage."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._log.debug(
                "Downloading DAGs from wasb://%s/%s to %s",
                self.container_name,
                self.prefix,
                self.wasb_dags_dir,
            )
            # List all blobs recursively for the given prefix
            blob_names = self.wasb_hook.get_blobs_list_recursive(
                container_name=self.container_name,
                prefix=self.prefix if self.prefix else None,
            )

            downloaded_files: list[Path] = []
            for blob_name in blob_names:
                # Skip storage directory markers
                if blob_name.endswith("/"):
                    continue

                # Compute relative path from prefix
                if self.prefix and blob_name.startswith(self.prefix):
                    relative_path = blob_name[len(self.prefix) :].lstrip("/")
                else:
                    relative_path = blob_name

                local_target_path = self.wasb_dags_dir / relative_path

                # Create parent directories if needed
                if not local_target_path.parent.exists():
                    local_target_path.parent.mkdir(parents=True, exist_ok=True)

                # Download the blob
                self.wasb_hook.get_file(
                    file_path=str(local_target_path),
                    container_name=self.container_name,
                    blob_name=blob_name,
                )
                downloaded_files.append(local_target_path)

            # Remove stale local files that no longer exist in the container
            if self.wasb_dags_dir.exists():
                for existing_file in self.wasb_dags_dir.rglob("*"):
                    if existing_file.is_file() and existing_file not in downloaded_files:
                        self._log.debug("Removing stale file: %s", existing_file)
                        existing_file.unlink()

                # Remove empty directories
                for existing_dir in sorted(self.wasb_dags_dir.rglob("*"), reverse=True):
                    if existing_dir.is_dir() and not any(existing_dir.iterdir()):
                        self._log.debug("Removing empty directory: %s", existing_dir)
                        existing_dir.rmdir()

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the DAGs in Azure Blob Storage. Currently, versioning is not supported.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the DAGs in Azure Blob Storage. Currently, versioning is not supported."""
        if self.version:
            raise AirflowException("Azure Blob Storage URL with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            return self._view_url_template
        # https://portal.azure.com/#view/Microsoft_Azure_Storage/BlobContainerBlade/...
        url = (
            f"https://portal.azure.com/#view/Microsoft_Azure_Storage/"
            f"BlobContainerBlade/~/storageAccountId/"
            f"%2Fsubscriptions%2F%2FresourceGroups%2F%2Fproviders%2F"
            f"Microsoft.Storage%2FstorageAccounts%2F"
            f"{self.container_name}%2FblobServices%2Fdefault%2Fcontainers%2F"
            f"{self.container_name}"
        )
        return url
