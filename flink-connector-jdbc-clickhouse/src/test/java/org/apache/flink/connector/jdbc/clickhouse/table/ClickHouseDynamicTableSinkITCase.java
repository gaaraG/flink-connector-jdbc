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

package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTableTestBase {

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(
                // upsertOutputTable,
                appendOutputTable, batchOutputTable, realOutputTable, checkpointOutputTable
                // userOutputTable
                );
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                field("id", DataTypes.INT().notNull()),
                field("num", DataTypes.BIGINT().notNull()),
                field("ts", dbType("DateTime64(3, 'UTC')"), DataTypes.TIMESTAMP(3)));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                field("NAME", DataTypes.VARCHAR(20).notNull()),
                field("SCORE", DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return tableRow("REAL_TABLE", field("real_data", dbType("REAL"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return tableRow("checkpointTable", field("id", DataTypes.BIGINT().notNull()));
    }

    @Disabled("ClickHouse dont allow create tables with PK")
    @Test
    @Override
    protected void testUpsert() throws Exception {
        DatabaseMetadata dbMeta = getMetadata();
        try (Connection conn = dbMeta.getConnection()) {
            try {
                upsertOutputTable.createTable(conn);
                super.testUpsert();
            } finally {
                upsertOutputTable.deleteTable(conn);
            }
        }
    }

    @Disabled("ClickHouse dont allow create tables with PK")
    @Test
    @Override
    protected void testReadingFromChangelogSource() throws Exception {
        try (Connection conn = getMetadata().getConnection()) {
            try {
                userOutputTable.createTable(conn);
                super.testReadingFromChangelogSource();
            } finally {
                userOutputTable.deleteTable(conn);
            }
        }
    }
}
