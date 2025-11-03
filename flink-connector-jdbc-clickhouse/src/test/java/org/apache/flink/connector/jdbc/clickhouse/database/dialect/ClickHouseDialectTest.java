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

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClickHouseDialect}. */
class ClickHouseDialectTest {

    private ClickHouseDialect dialect;

    @BeforeEach
    void setUp() {
        dialect = new ClickHouseDialect();
    }

    @Test
    void testDialectIdentity() {
        assertThat(dialect.dialectName()).isEqualTo("ClickHouse");
        assertThat(dialect.defaultDriverName())
                .hasValue("com.clickhouse.jdbc.ClickHouseDriver");
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
                        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        assertThat(dialect.supportedTypes()).isEqualTo(expected);
    }

    @Test
    void testDecimalPrecisionRange() {
        Optional<JdbcDialect.Range> range = dialect.decimalPrecisionRange();
        assertThat(range).isPresent();
        assertThat(range.get().min).isEqualTo(1);
        assertThat(range.get().max).isEqualTo(76);
    }

    @Test
    void testTimestampPrecisionRange() {
        Optional<JdbcDialect.Range> range = dialect.timestampPrecisionRange();
        assertThat(range).isPresent();
        assertThat(range.get().min).isEqualTo(0);
        assertThat(range.get().max).isEqualTo(9);
    }

    @Test
    void testDecimalPrecisionValidation() {
        RowType rowType = RowType.of(new DecimalType(77, 2));
        assertThatThrownBy(() -> dialect.validate(rowType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("precision range [1, 76]");
    }

    @Test
    void testTimestampPrecisionValidation() {
        RowType rowType = RowType.of(new TimestampType(10));
        assertThatThrownBy(() -> dialect.validate(rowType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("precision range [0, 9]");
    }
}

