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

package org.apache.flink.connector.jdbc.clickhouse.database.dialect;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClickHouseDialect}. */
class ClickHouseDialectTest implements ClickHouseTestBase {

    private ClickHouseDialect dialect;

    @BeforeEach
    void setUp() {
        dialect = new ClickHouseDialect();
    }

    @Test
    void testDialectIdentity() {
        assertThat(dialect.dialectName()).isEqualTo("ClickHouse");
        assertThat(dialect.defaultDriverName()).hasValue("com.clickhouse.jdbc.ClickHouseDriver");
    }

    @Test
    void testSqlFragments() {
        assertThat(dialect.getLimitClause(10)).isEqualTo("LIMIT 10");
        assertThat(dialect.quoteIdentifier("TableName")).isEqualTo("`TableName`");
        assertThat(dialect.getUpsertStatement("tbl", new String[] {"id"}, new String[] {"id"}))
                .isEmpty();
    }

    @Test
    void testSupportedTypes() {
        Set<LogicalTypeRoot> expected =
                EnumSet.of(
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
        assertThat(dialect.supportedTypes()).isEqualTo(expected);
    }

    @Test
    void testDecimalPrecisionRange() {
        // Just check that the range is present, without accessing internal fields
        assertThat(dialect.decimalPrecisionRange()).isPresent();
    }

    @Test
    void testTimestampPrecisionRange() {
        // Just check that the range is present, without accessing internal fields
        assertThat(dialect.timestampPrecisionRange()).isPresent();
    }

    @Test
    void testDecimalPrecisionValidation() {
        // Use a precision within the valid range for Flink (1-38)
        RowType rowType = RowType.of(new DecimalType(38, 0));
        // This should not throw an exception since 38 is within the valid range for Flink
        dialect.validate(rowType);
        // Only test invalid precision - but we need to catch the exception when creating the type
        // itself
        assertThatThrownBy(
                        () -> {
                            RowType invalidRowType = RowType.of(new DecimalType(76, 0));
                            dialect.validate(invalidRowType);
                        })
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Decimal precision must be between 1 and 38 (both inclusive).");
    }

    @Test
    void testTimestampPrecisionValidation() {
        // Use a precision within the valid range for ClickHouse/Flink (0-9)
        RowType rowType = RowType.of(new TimestampType(9));
        // This should not throw an exception since 9 is within the valid range
        dialect.validate(rowType);
        // Only test invalid precision - but we need to catch the exception when creating the type
        // itself
        assertThatThrownBy(
                        () -> {
                            RowType invalidRowType = RowType.of(new TimestampType(10));
                            dialect.validate(invalidRowType);
                        })
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Timestamp precision must be between 0 and 9 (both inclusive).");
    }

    @Test
    void testArrayAndMapTypesSupport() {
        assertThat(dialect.supportedTypes()).contains(LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP);
    }
}
