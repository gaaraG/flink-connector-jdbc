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

package org.apache.flink.connector.jdbc.clickhouse;

import org.apache.flink.connector.jdbc.clickhouse.database.catalog.ClickHouseCatalog;
import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.database.JdbcFactoryLoader;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for ClickHouse connector components. */
class ClickHouseIntegrationTest implements ClickHouseTestBase {

    @Test
    void testDialectLoading() {
        JdbcDialect dialect =
                JdbcFactoryLoader.loadDialect(
                        getMetadata().getJdbcUrl(), Thread.currentThread().getContextClassLoader());

        assertThat(dialect).isNotNull();
        assertThat(dialect).isInstanceOf(ClickHouseDialect.class);
        assertThat(dialect.dialectName()).isEqualTo("ClickHouse");
    }

    @Test
    void testCatalogCreation() {
        JdbcCatalog catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "test_catalog",
                        "default",
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata().getJdbcUrl());

        assertThat(catalog).isNotNull();
        assertThat(catalog).isInstanceOf(ClickHouseCatalog.class);
    }

    @Test
    void testBasicDatabaseOperations() throws Exception {
        // Test that we can connect to the database
        Properties properties =
                getBriefAuthProperties(getMetadata().getUsername(), getMetadata().getPassword());

        try (Connection conn =
                DriverManager.getConnection(getMetadata().getJdbcUrl(), properties)) {

            // Test basic query
            PreparedStatement stmt = conn.prepareStatement("SELECT 1");
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void testDialectTypeSupport() {
        ClickHouseDialect dialect = new ClickHouseDialect();

        // Test that the dialect supports common types
        assertThat(dialect.supportedTypes())
                .contains(
                        LogicalTypeRoot.CHAR,
                        LogicalTypeRoot.VARCHAR,
                        LogicalTypeRoot.BOOLEAN,
                        LogicalTypeRoot.VARBINARY,
                        LogicalTypeRoot.DECIMAL,
                        LogicalTypeRoot.TINYINT,
                        LogicalTypeRoot.SMALLINT,
                        LogicalTypeRoot.INTEGER,
                        LogicalTypeRoot.BIGINT,
                        LogicalTypeRoot.FLOAT,
                        LogicalTypeRoot.DOUBLE,
                        LogicalTypeRoot.DATE,
                        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                        LogicalTypeRoot.ARRAY,
                        LogicalTypeRoot.MAP);
    }
}
