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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** ClickHouseTypeMapper util class. */
@Internal
public class ClickHouseTypeMapper implements JdbcCatalogTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTypeMapper.class);

    // ============================data types=====================

    // Boolean
    private static final String CK_BOOL = "BOOL";

    // -------------------------integer---------------------------
    private static final String CK_INT8 = "INT8";
    private static final String CK_UINT8 = "UINT8";
    private static final String CK_INT16 = "INT16";
    private static final String CK_UINT16 = "UINT16";
    private static final String CK_INT32 = "INT32";
    private static final String CK_UINT32 = "UINT32";
    private static final String CK_INT64 = "INT64";
    private static final String CK_UINT64 = "UINT64";
    private static final String CK_INT128 = "INT128";
    private static final String CK_INT256 = "INT256";
    private static final String CK_UINT128 = "UINT128";
    private static final String CK_UINT256 = "UINT256";

    // -------------------------float/decimal---------------------
    private static final String CK_FLOAT32 = "FLOAT32";
    private static final String CK_FLOAT64 = "FLOAT64";
    private static final String CK_DECIMAL = "DECIMAL";
    private static final String CK_DECIMAL32 = "DECIMAL32";
    private static final String CK_DECIMAL64 = "DECIMAL64";
    private static final String CK_DECIMAL128 = "DECIMAL128";
    private static final String CK_DECIMAL256 = "DECIMAL256";

    // -------------------------string----------------------------
    private static final String CK_STRING = "STRING";
    private static final String CK_FIXED_STRING = "FIXEDSTRING";
    private static final String CK_UUID = "UUID";
    private static final String CK_IPV4 = "IPV4";
    private static final String CK_IPV6 = "IPV6";
    private static final String CK_ENUM8 = "ENUM8";
    private static final String CK_ENUM16 = "ENUM16";

    // ------------------------------time-------------------------
    private static final String CK_DATE = "DATE";
    private static final String CK_DATE32 = "DATE32";
    private static final String CK_DATETIME = "DATETIME";
    private static final String CK_DATETIME64 = "DATETIME64";

    // ------------------------------arrays---------------------
    private static final String CK_ARRAY = "ARRAY";

    // ------------------------------maps-----------------------
    private static final String CK_MAP = "MAP";

    // ------------------------------wrappers---------------------
    private static final String CK_NULLABLE = "NULLABLE";
    private static final String CK_LOW_CARDINALITY = "LOWCARDINALITY";

    private final String databaseVersion;
    private final String driverVersion;

    public ClickHouseTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }
    // https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-data/src/main/java/com/clickhouse/data/ClickHouseDataType.java
    private DataType toFlinkType(String rawType, String columnName) {
        String fullType = normalizeType(rawType).trim().toUpperCase();
        String baseType = extractBaseName(fullType);
        switch (baseType) {
            case CK_BOOL:
                return DataTypes.BOOLEAN();
            case CK_INT8:
                return DataTypes.TINYINT();
            case CK_UINT8:
            case CK_INT16:
                return DataTypes.SMALLINT();
            case CK_UINT16:
            case CK_INT32:
                return DataTypes.INT();
            case CK_UINT32:
            case CK_INT64:
                return DataTypes.BIGINT();
            case CK_UINT64:
                // UInt64 max value (18446744073709551615) exceeds Java Long.MAX_VALUE.
                // We use DECIMAL(20, 0) to hold the full range safely.
                return DataTypes.DECIMAL(20, 0);
            case CK_INT128:
            case CK_UINT128:
            case CK_INT256:
            case CK_UINT256:
            case CK_DECIMAL:
            case CK_DECIMAL32:
            case CK_DECIMAL64:
            case CK_DECIMAL128:
            case CK_DECIMAL256:
                // Flink's DecimalType only supports a maximum precision of 38.
                // ClickHouse's Decimal256 can have a precision up to 76.
                // If the precision exceeds Flink's limit, we fallback to STRING to avoid runtime
                // exceptions.
                int[] ps = parsePrecisionScale(fullType);
                int precision = ps[0];
                int scale = ps[1];
                if (precision > 38) {
                    LOG.warn(
                            "Type '{}' for column '{}' has a precision of {}, which exceeds Flink's maximum limit of 38. "
                                    + "Mapping to STRING to preserve data.",
                            fullType,
                            columnName,
                            precision);
                    return DataTypes.STRING();
                }
                return DataTypes.DECIMAL(precision, scale);
            case CK_FLOAT32:
                return DataTypes.FLOAT();
            case CK_FLOAT64:
                return DataTypes.DOUBLE();
            case CK_STRING:
            case CK_FIXED_STRING:
            case CK_ENUM8:
            case CK_ENUM16:
            case CK_UUID:
            case CK_IPV4:
            case CK_IPV6:
                return DataTypes.STRING();

            case CK_DATE:
            case CK_DATE32:
                return DataTypes.DATE();

            case CK_DATETIME:
                // DateTime is second precision
                return DataTypes.TIMESTAMP(0);
            case CK_DATETIME64:
                // DateTime64(S), scale usually represents S
                int timeScale = parseScaleFromDateTime64(fullType);
                return DataTypes.TIMESTAMP(timeScale);
            case CK_ARRAY:
                // Handle array types
                return handleArrayType(fullType, columnName);
            case CK_MAP:
                // Handle map types
                return handleMapType(fullType, columnName);

            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support ClickHouse type '%s' (raw: '%s') on column '%s' in ClickHouse version %s, driver version %s yet.",
                                fullType, rawType, columnName, databaseVersion, driverVersion));
        }
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String rawType = metadata.getColumnTypeName(colIndex);
        final String columnName = metadata.getColumnName(colIndex);

        return toFlinkType(rawType, columnName);
    }

    private DataType handleArrayType(String fullType, String columnName) {
        String innerContent = extractInnerType(fullType);
        if (innerContent.isEmpty()) {
            return DataTypes.ARRAY(DataTypes.STRING());
        }
        DataType elementType = toFlinkType(innerContent, columnName);
        return DataTypes.ARRAY(elementType);
    }

    private DataType handleMapType(String fullType, String columnName) {
        String innerContent = extractInnerType(fullType);
        if (innerContent.isEmpty()) {
            return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
        }

        List<String> types = splitTopLevel(innerContent);
        if (types.size() != 2) {
            return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
        }

        DataType keyType = toFlinkType(types.get(0), columnName);
        DataType valueType = toFlinkType(types.get(1), columnName);

        return DataTypes.MAP(keyType, valueType);
    }

    /**
     * Recursively unwraps ClickHouse types like "Nullable(Int32)" or
     * "LowCardinality(Nullable(String))".
     */
    private String normalizeType(String typeName) {
        if (typeName == null) {
            return "";
        }
        String upper = typeName.trim().toUpperCase();

        if (upper.startsWith(CK_NULLABLE) || upper.startsWith(CK_LOW_CARDINALITY)) {
            return normalizeType(extractInnerType(typeName));
        }
        return typeName;
    }

    private String extractInnerType(String typeName) {
        int start = typeName.indexOf('(');
        int end = typeName.lastIndexOf(')');
        if (start > 0 && end > start) {
            return typeName.substring(start + 1, end);
        }
        return typeName;
    }

    /**
     * Extracts base name by removing parameters. e.g., "DateTime64(3, 'Asia/Shanghai')" ->
     * "DATETIME64"
     */
    private String extractBaseName(String fullType) {
        int idx = fullType.indexOf('(');
        if (idx > 0) {
            return fullType.substring(0, idx).trim();
        }
        return fullType;
    }

    private int[] parsePrecisionScale(String fullType) {
        int p = 38;
        int s = 18;

        try {
            String content = extractInnerType(fullType);
            if (!content.isEmpty()) {
                String[] parts = content.split(",");
                if (parts.length == 2) {
                    p = Integer.parseInt(parts[0].trim());
                    s = Integer.parseInt(parts[1].trim());
                } else if (parts.length == 1) {
                    p = Integer.parseInt(parts[0].trim());
                    s = 0;
                }
            }
        } catch (NumberFormatException e) {
            LOG.warn("Failed to parse precision/scale from '{}', using default (38, 18)", fullType, e);
        }
        return new int[] {p, s};
    }

    private int parseScaleFromDateTime64(String fullType) {
        int s = 3;
        try {
            String content = extractInnerType(fullType);
            if (!content.isEmpty()) {
                List<String> parts = splitTopLevel(content);
                if (!parts.isEmpty()) {
                    s = Integer.parseInt(parts.get(0).trim());
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return s;
    }

    private List<String> splitTopLevel(String input) {
        List<String> result = new ArrayList<>();
        int balance = 0;
        StringBuilder current = new StringBuilder();

        for (char c : input.toCharArray()) {
            if (c == '(') {
                balance++;
            } else if (c == ')') {
                balance--;
            }

            if (c == ',' && balance == 0) {
                result.add(current.toString().trim());
                current.setLength(0); // 清空 buffer
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            result.add(current.toString().trim());
        }
        return result;
    }
}
