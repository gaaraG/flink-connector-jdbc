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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link ClickHouseDialectConverter}. */
class ClickHouseDialectConverterTest implements ClickHouseTestBase {

    @Test
    void testArrayConversionFromResultSet() throws SQLException {
        // Test array of integers conversion from ResultSet
        ArrayType intArrayType = new ArrayType(new IntType());
        RowType rowType = RowType.of(new ArrayType[] {intArrayType}, new String[] {"int_array"});

        ClickHouseDialectConverter converter = new ClickHouseDialectConverter(rowType);

        // Create mock ResultSet with array data
        ResultSet resultSet = mock(ResultSet.class);
        Array mockArray = mock(Array.class);
        Integer[] intArray = {1, 2, 3, 4, 5};

        when(resultSet.getObject(1)).thenReturn(mockArray);
        when(mockArray.getArray()).thenReturn(intArray);

        // Convert and check
        RowData rowData = converter.toInternal(resultSet);
        ArrayData resultArray = rowData.getArray(0);
        assertThat(resultArray).isNotNull();
        assertThat(resultArray.size()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(resultArray.getInt(i)).isEqualTo(i + 1);
        }
    }

    @Test
    void testStringArrayConversionFromResultSet() throws SQLException {
        // Test array of strings conversion from ResultSet
        ArrayType stringArrayType = new ArrayType(new VarCharType());
        RowType rowType =
                RowType.of(new ArrayType[] {stringArrayType}, new String[] {"string_array"});

        ClickHouseDialectConverter converter = new ClickHouseDialectConverter(rowType);

        // Create mock ResultSet with string array data
        ResultSet resultSet = mock(ResultSet.class);
        Array mockArray = mock(Array.class);
        String[] stringArray = {"first", "second", "third"};

        when(resultSet.getObject(1)).thenReturn(mockArray);
        when(mockArray.getArray()).thenReturn(stringArray);

        // Convert and check
        RowData rowData = converter.toInternal(resultSet);
        ArrayData resultArray = rowData.getArray(0);
        assertThat(resultArray).isNotNull();
        assertThat(resultArray.size()).isEqualTo(3);
        assertThat(resultArray.getString(0).toString()).isEqualTo("first");
        assertThat(resultArray.getString(1).toString()).isEqualTo("second");
        assertThat(resultArray.getString(2).toString()).isEqualTo("third");
    }
}
