/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.clickhouse.database.catalog;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link ClickHouseCatalog}. */
class ClickHouseCatalogITCase implements ClickHouseTestBase {

    @Test
    void testCreateCatalog() {
        JdbcCatalog catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "clickhouse_catalog",
                        "default",
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata().getJdbcUrl());

        assertThat(catalog).isNotNull();
    }

    @Test
    void testListDatabases() {
        JdbcCatalog catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "clickhouse_catalog",
                        "default",
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata().getJdbcUrl());

        // Initialize the catalog
        catalog.open();

        List<String> databases = catalog.listDatabases();
        assertThat(databases).isNotNull();
        // Should contain at least the default database
        assertThat(databases).contains("default");
    }

    @Test
    void testDatabaseExists() {
        JdbcCatalog catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "clickhouse_catalog",
                        "default",
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata().getJdbcUrl());

        // Initialize the catalog
        catalog.open();

        assertThat(catalog.databaseExists("default")).isTrue();
        assertThat(catalog.databaseExists("non_existing_db")).isFalse();
    }

    @Test
    void testNonExistingDatabase() throws Exception {
        JdbcCatalog catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "clickhouse_catalog",
                        "default",
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata().getJdbcUrl());

        // Initialize the catalog
        catalog.open();

        assertThatThrownBy(() -> catalog.listTables("non_existing_db"))
                .isInstanceOf(DatabaseNotExistException.class);
    }
}
