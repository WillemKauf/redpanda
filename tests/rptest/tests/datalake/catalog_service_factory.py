# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.catalog_service import CatalogType, CatalogService, CatalogImpl
from rptest.services.nessie_catalog import NessieCatalog
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog

SUPPORTED_CATALOGS = [NessieCatalog, IcebergRESTCatalog]
SUPPORTED_CATALOG_TYPES_AND_IMPLS = [(CatalogType.REST, CatalogImpl.JDBC),
                                     (CatalogType.REST, CatalogImpl.HADOOP),
                                     (CatalogType.NESSIE, None)]


def supported_catalog_types_and_impls():
    return SUPPORTED_CATALOG_TYPES_AND_IMPLS


def get_catalog_service_by_type(catalog_type: CatalogType):
    for svc in SUPPORTED_CATALOGS:
        if svc.catalog_type() == catalog_type:
            return svc
    raise NotImplementedError(f"No catalog of type {catalog_type}")
