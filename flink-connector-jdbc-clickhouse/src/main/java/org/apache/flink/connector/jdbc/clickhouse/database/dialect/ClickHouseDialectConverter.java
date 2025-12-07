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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * ClickHouse.
 */
@Internal
public class ClickHouseDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    public ClickHouseDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
                return val -> {
                    if (val instanceof UUID) {
                        return StringData.fromString(val.toString());
                    } else if (val instanceof InetAddress) {
                        return StringData.fromString(((InetAddress) val).getHostAddress());
                    } else {
                        return StringData.fromString(val.toString());
                    }
                };
            case DATE:
                return val ->
                        val instanceof LocalDate
                                ? (int) ((LocalDate) val).toEpochDay()
                                : (int) ((Date) val).toLocalDate().toEpochDay();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof OffsetDateTime
                                ? TimestampData.fromLocalDateTime(
                                        ((OffsetDateTime) val).toLocalDateTime())
                                : val instanceof LocalDateTime
                                        ? TimestampData.fromLocalDateTime((LocalDateTime) val)
                                        : TimestampData.fromTimestamp((Timestamp) val);
            case ARRAY:
                return createArrayInternalConverter((ArrayType) type);
            case MAP:
                return createMapInternalConverter((MapType) type);
            default:
                return super.createInternalConverter(type);
        }
    }

    private JdbcDeserializationConverter createArrayInternalConverter(ArrayType type) {
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(type.getElementType());
        final JdbcDeserializationConverter elementConverter =
                createInternalConverter(type.getElementType());
        return val -> {
            if (val instanceof java.sql.Array) {
                try {
                    Object[] in = (Object[]) ((java.sql.Array) val).getArray();
                    final Object[] convertedArray =
                            (Object[]) Array.newInstance(elementClass, in.length);
                    for (int i = 0; i < convertedArray.length; i++) {
                        convertedArray[i] = elementConverter.deserialize(in[i]);
                    }
                    return new GenericArrayData(convertedArray);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to convert array", e);
                }
            }

            return val;
        };
    }

    private JdbcDeserializationConverter createMapInternalConverter(MapType type) {
        final JdbcDeserializationConverter keyConverter =
                createInternalConverter(type.getKeyType());
        final JdbcDeserializationConverter valueConverter =
                createInternalConverter(type.getValueType());
        return val -> {
            if (val instanceof Map) {
                try {
                    Map<?, ?> jdbcMap = (Map<?, ?>) val;
                    Map<Object, Object> map = new HashMap<>();
                    for (Map.Entry<?, ?> entry : jdbcMap.entrySet()) {
                        Object key = keyConverter.deserialize(entry.getKey());
                        Object value = valueConverter.deserialize(entry.getValue());
                        map.put(key, value);
                    }
                    return new GenericMapData(map);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to convert map", e);
                }
            }
            return val;
        };
    }

    @Override
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.ARRAY) {
            return createArrayExternalConverter((ArrayType) type);

        } else if (root == LogicalTypeRoot.MAP) {
            return createMapExternalConverter((MapType) type);
        } else {
            return super.createNullableExternalConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index,
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case ARRAY:
                return createArrayExternalConverter((ArrayType) type);
            case MAP:
                return createMapExternalConverter((MapType) type);
            default:
                return super.createExternalConverter(type);
        }
    }

    private JdbcSerializationConverter createArrayExternalConverter(ArrayType type) {
        final LogicalType elementType = type.getElementType();
        return (val, index, statement) -> {
            if (val.isNullAt(index)) {
                statement.setObject(index, null);
                return;
            }
            ArrayData arrayData = val.getArray(index);
            Object[] javaArray = convertArrayDataToJavaArray(arrayData, elementType);
            statement.setObject(index, javaArray);
        };
    }

    private JdbcSerializationConverter createMapExternalConverter(MapType type) {
        final LogicalType keyType = type.getKeyType();
        final LogicalType valueType = type.getValueType();
        return (JdbcSerializationConverter & Serializable)
                (val, index, statement) -> {
                    if (val.isNullAt(index)) {
                        statement.setObject(index, null);
                        return;
                    }
                    MapData mapData = val.getMap(index);
                    Map<Object, Object> javaMap =
                            convertMapDataToJavaMap(mapData, keyType, valueType);
                    statement.setObject(index, javaMap);
                };
    }

    // --------------------------------------------------------------------------------------
    // Helper Methods for Data Conversion (Flink Internal Data -> Java Standard Object)
    // --------------------------------------------------------------------------------------

    private static Object[] convertArrayDataToJavaArray(
            ArrayData arrayData, LogicalType elementType) {
        int size = arrayData.size();
        Object[] javaArray = new Object[size];
        for (int i = 0; i < size; i++) {
            if (arrayData.isNullAt(i)) {
                javaArray[i] = null;
            } else {
                javaArray[i] = getElementFromAccessor(arrayData, i, elementType);
            }
        }
        return javaArray;
    }

    private static Map<Object, Object> convertMapDataToJavaMap(
            MapData mapData, LogicalType keyType, LogicalType valueType) {
        Map<Object, Object> javaMap = new HashMap<>();
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        int size = mapData.size();

        for (int i = 0; i < size; i++) {
            Object key = keyArray.isNullAt(i) ? null : getElementFromAccessor(keyArray, i, keyType);
            Object value =
                    valueArray.isNullAt(i)
                            ? null
                            : getElementFromAccessor(valueArray, i, valueType);
            javaMap.put(key, value);
        }
        return javaMap;
    }

    /**
     * Extracts a single element from Flink ArrayData (which is also used for Map keys/values) and
     * converts it to a standard Java Object compatible with JDBC.
     */
    private static Object getElementFromAccessor(ArrayData accessor, int pos, LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return accessor.getString(pos).toString();
            case BOOLEAN:
                return accessor.getBoolean(pos);
            case TINYINT:
                return accessor.getByte(pos);
            case SMALLINT:
                return accessor.getShort(pos);
            case INTEGER:
            case BIGINT:
                return accessor.getInt(pos);
            case FLOAT:
                return accessor.getFloat(pos);
            case DOUBLE:
                return accessor.getDouble(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return accessor.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay(accessor.getInt(pos)));
            case TIME_WITHOUT_TIME_ZONE:
                return Time.valueOf(
                        java.time.LocalTime.ofNanoOfDay(accessor.getInt(pos) * 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return accessor.getTimestamp(pos, 3)
                        .toTimestamp(); // Defaulting to ms precision, verify if needed
            case ARRAY:
                // Recursive call for nested arrays
                return convertArrayDataToJavaArray(
                        accessor.getArray(pos), ((ArrayType) type).getElementType());
            case MAP:
                // Recursive call for nested maps
                MapType mapType = (MapType) type;
                return convertMapDataToJavaMap(
                        accessor.getMap(pos), mapType.getKeyType(), mapType.getValueType());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for ClickHouse array/map conversion: " + type);
        }
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
