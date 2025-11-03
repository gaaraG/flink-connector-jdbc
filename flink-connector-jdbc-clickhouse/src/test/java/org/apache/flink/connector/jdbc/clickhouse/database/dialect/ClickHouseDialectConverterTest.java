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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClickHouseDialectConverter}. */
class ClickHouseDialectConverterTest {

    @Test
    void testDateAndTimestampConversions() throws Exception {
        RowType rowType =
                RowType.of(
                        new DateType(), new TimestampType(3), new LocalZonedTimestampType(6));
        ClickHouseDialectConverter converter = new ClickHouseDialectConverter(rowType);

        ResultSet resultSet = Mockito.mock(ResultSet.class);
        LocalDate localDate = LocalDate.of(2024, 5, 18);
        OffsetDateTime offsetDateTime =
                OffsetDateTime.of(LocalDateTime.of(2024, 5, 18, 1, 2, 3, 456_000_000), ZoneOffset.UTC);
        OffsetDateTime offsetDateTimeWithZone =
                OffsetDateTime.of(
                        LocalDateTime.of(2024, 5, 18, 6, 7, 8, 901_234_000), ZoneOffset.ofHours(8));

        Mockito.when(resultSet.getObject(1)).thenReturn(localDate);
        Mockito.when(resultSet.getObject(2)).thenReturn(offsetDateTime);
        Mockito.when(resultSet.getObject(3)).thenReturn(offsetDateTimeWithZone);

        RowData rowData = converter.toInternal(resultSet);

        assertThat(rowData.getInt(0)).isEqualTo((int) localDate.toEpochDay());
        assertThat(rowData.getTimestamp(1, 3))
                .isEqualTo(TimestampData.fromLocalDateTime(offsetDateTime.toLocalDateTime()));
        assertThat(rowData.getTimestamp(2, 6))
                .isEqualTo(
                        TimestampData.fromLocalDateTime(
                                offsetDateTimeWithZone.toLocalDateTime()));
    }
}

