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

package org.apache.flink.connector.jdbc.clickhouse.database;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.clickhouse.database.catalog.ClickHouseCatalog;
import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClickHouseFactory}. */
class ClickHouseFactoryTest implements ClickHouseTestBase {

    private final ClickHouseFactory factory = new ClickHouseFactory();

    @Test
    void testAcceptsSupportedUrls() {
        assertThat(factory.acceptsURL("jdbc:clickhouse://localhost:8123/db")).isTrue();
        assertThat(factory.acceptsURL("jdbc:ch://localhost:9000/db")).isTrue();
    }

    @Test
    void testRejectsUnsupportedUrls() {
        assertThat(factory.acceptsURL(null)).isFalse();
        assertThat(factory.acceptsURL("jdbc:mysql://localhost:3306/db")).isFalse();
        assertThat(factory.acceptsURL("jdbc:postgresql://localhost:5432/db")).isFalse();
    }

    @Test
    void testCreateDialect() {
        assertThat(factory.createDialect()).isInstanceOf(ClickHouseDialect.class);
    }

    @Test
    void testCreateCatalog() {
        JdbcCatalog catalog =
                factory.createCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "clickhouse_catalog",
                        "default",
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata().getJdbcUrl());

        assertThat(catalog).isNotNull();
        // Simply check that the catalog was created successfully
        assertThat(catalog).isInstanceOf(ClickHouseCatalog.class);
    }
}
