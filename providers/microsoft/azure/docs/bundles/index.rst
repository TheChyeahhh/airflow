.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Bundles
#######

Dag bundles allow Airflow to load Dags from external sources. For a general overview see
:doc:`apache-airflow:administration-and-deployment/dag-bundles`.

AzureBlobStorageDagBundle
=========================

Use the :class:`~airflow.providers.microsoft.azure.bundles.wasb.AzureBlobStorageDagBundle` to configure an
Azure Blob Storage bundle in your Airflow's ``[dag_processor] dag_bundle_config_list``.

Example of using the AzureBlobStorageDagBundle:

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
      {
        "name": "my-azure-dags",
        "classpath": "airflow.providers.microsoft.azure.bundles.wasb.AzureBlobStorageDagBundle",
        "kwargs": {
          "wasb_conn_id": "wasb_default",
          "container_name": "my-airflow-container",
          "prefix": "dags/",
          "refresh_interval": 60
        }
      }
    ]'