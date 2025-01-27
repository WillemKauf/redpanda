# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service
from ducktape.cluster.cluster import ClusterNode
from rptest.context import cloud_storage

from typing import Optional, Any
from enum import Enum

from pyiceberg.catalog import load_catalog


class CatalogType(str, Enum):
    REST = 'rest'
    POLARIS = 'polaris'



class CatalogService(Service):
    # Expected to be available after initialization of derived class.
    # Use catalog_url property to access.
    _catalog_url: Optional[str] = None
    _warehouse_name = 'redpanda'

    def __init__(self,
                 ctx,
                 cloud_storage_bucket: str,
                 catalog_name: str = 'redpanda-iceberg-catalog',
                 node: ClusterNode | None = None):
        super(CatalogService, self).__init__(ctx, num_nodes=0 if node else 1)
        self.dedicated_nodes = ctx.globals.get("dedicated_nodes", False)
        self.credentials = cloud_storage.Credentials.from_context(ctx)

        self.cloud_storage_bucket = cloud_storage_bucket
        self.catalog_name = catalog_name
        self._catalog_url = None

    @property
    def catalog_url(self) -> str:
        assert self._catalog_url, "URL not available because service is not started"
        return self._catalog_url

    def compute_warehouse_path(self):
        if isinstance(self.credentials,
                      cloud_storage.S3Credentials) or isinstance(
                          self.credentials,
                          cloud_storage.AWSInstanceMetadataCredentials):
            s3_prefix = "s3"
            self.cloud_storage_warehouse = f"{s3_prefix}://{self.cloud_storage_bucket}/{self.catalog_name}"
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            self.cloud_storage_warehouse = f"gs://{self.cloud_storage_bucket}/{self.catalog_name}"
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            self.cloud_storage_warehouse = f"abfss://{self.cloud_storage_bucket}@{self.credentials.endpoint}/{self.catalog_name}"
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

    def client(self, catalog_name: Optional[str] = None):
        if not catalog_name:
            catalog_name = self.catalog_name

        conf = dict()
        conf["uri"] = self.catalog_url

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            conf["s3.endpoint"] = self.credentials.endpoint
            conf["s3.access-key-id"] = self.credentials.access_key
            conf["s3.secret-access-key"] = self.credentials.secret_key
            conf["s3.region"] = self.credentials.region
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.GCPInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            # Legancy pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adlfs.account-name"] = self.credentials.account_name
            conf["adlfs.account-key"] = self.credentials.account_key
            # Modern pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adls.account-name"] = self.credentials.account_name
            conf["alds.account-key"] = self.credentials.account_key
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

        return load_catalog(catalog_name, **conf)
