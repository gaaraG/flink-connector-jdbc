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
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/** ITCase for {@link ClickHouseCatalog}. */
class ClickHouseCatalogITCase implements ClickHouseTestBase {
    private static final String TEST_CATALOG_NAME = "clickhouse_catalog";
    private static final String TEST_DB = "default";
    private ClickHouseCatalog catalog;
    private TableEnvironment tEnv;
    private final TableRow testTable = createTableAllTypeTable("comprehensive_data_types_demo");
    private final TableRow testSinkTable =
            createTableAllTypeTable("comprehensive_data_types_demo_sink");
    private final String data1 = UUID.randomUUID().toString();
    private final String data2 = UUID.randomUUID().toString();

    private final List<Row> testData =
            List.of(
                    Row.of(
                            new BigDecimal("1"),
                            (short) 100,
                            "999999999999999999999999999999999999999",
                            -1.0001f,
                            1.7976931348623157e+308d,
                            new BigDecimal("9999999999999.99999"),
                            "9999999999999999999999999999999999999.9999999999",
                            "Max Value Test",
                            StringUtils.rightPad("MAX-VAL", 8),
                            LocalDate.parse("2025-11-01"),
                            LocalDateTime.parse("2025-11-01T10:00:00.999"),
                            data1,
                            true,
                            "web",
                            new Double[] {90.5d, 88.0d},
                            new String[] {"Initial", "FirstChange"},
                            Map.of("language", "en"),
                            null,
                            99.9d,
                            "US",
                            "192.168.1.1"),
                    Row.of(
                            new BigDecimal("2"),
                            (short) -100,
                            "-999999999999999999999999999999999999999",
                            1.212000f,
                            -1.7976931348623157e+308d,
                            new BigDecimal("-9999999999999.99999"),
                            "-9999999999999999999999999999.9999999999",
                            "Min Value Test",
                            StringUtils.rightPad("MIN-VAL", 8),
                            LocalDate.parse("2025-11-02"),
                            LocalDateTime.parse("2025-11-02T15:30:00.001"),
                            data2,
                            false,
                            "mobile",
                            new Double[] {100.0d},
                            new String[] {"Initial"},
                            Map.of("city", "Tokyo"),
                            "Bob Smith",
                            null,
                            "JP",
                            "10.0.0.5"));

    private TableRow createTableAllTypeTable(String tableName) {
        return tableRow(
                tableName,
                field("id", dbType("UInt64"), DataTypes.STRING()),
                field("small_int", dbType("Int16"), DataTypes.SMALLINT()),
                field("large_int", dbType("Int256"), DataTypes.STRING()),
                field("standard_float32", dbType("Float32"), DataTypes.FLOAT()),
                field("standard_float64", dbType("Float64"), DataTypes.DOUBLE()),
                field("precise_amount", dbType("Decimal64(5)"), DataTypes.DECIMAL(18, 5)),
                field("super_decimal", dbType("Decimal256(10)"), DataTypes.STRING()),
                field("var_string", dbType("String"), DataTypes.STRING()),
                field("fixed_code", dbType("FixedString(8)"), DataTypes.STRING()),
                field("simple_date", dbType("Date"), DataTypes.DATE()),
                field("datetime_ms", dbType("DateTime64(3, 'UTC')"), DataTypes.TIMESTAMP(3)),
                field("user_uuid", dbType("UUID"), DataTypes.STRING()),
                field("is_enabled", dbType("Bool"), DataTypes.BOOLEAN()),
                field(
                        "traffic_source",
                        dbType("Enum16('web' = 1, 'mobile' = 2, 'api' = 3)"),
                        DataTypes.STRING()),
                field("score_array", dbType("Array(Float64)"), DataTypes.ARRAY(DataTypes.DOUBLE())),
                field("name_history", dbType("Array(String)"), DataTypes.ARRAY(DataTypes.STRING())),
                field(
                        "user_settings",
                        dbType("Map(String, String)"),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                field("optional_name", dbType("Nullable(String)"), DataTypes.STRING().nullable()),
                field("optional_score", dbType("Nullable(Float64)"), DataTypes.DOUBLE().nullable()),
                field("country_code", dbType("LowCardinality(String)"), DataTypes.STRING()),
                field("ip_address", dbType("IPv4"), DataTypes.STRING()));
    }

    @BeforeEach
    void setup() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            st.execute(testTable.getCreateQuery());
            st.execute(testSinkTable.getCreateQuery());
            testTable.insertIntoTableValues(conn, testData);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        catalog =
                new ClickHouseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        TEST_DB,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata()
                                .getJdbcUrl()
                                .substring(0, getMetadata().getJdbcUrl().lastIndexOf("/")));
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @AfterEach
    void afterEach() {
        try (Connection conn = getMetadata().getConnection()) {
            testTable.dropTable(conn);
            testSinkTable.dropTable(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSelectData() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", testTable.getTableName()))
                                .execute()
                                .collect());
        assertRowsEquals(testData, results);
    }

    protected void assertRowsEquals(List<Row> expected, List<Row> actual) {
        Assertions.assertEquals(expected.size(), actual.size());

        for (int i = 0; i < expected.size(); i++) {
            Row r1 = expected.get(i);
            Row r2 = actual.get(i);
            for (int fi = 0; fi < r1.getArity(); fi++) {
                Object o1 = r1.getField(fi);
                Object o2 = r2.getField(fi);
                String s1 = normalizeObjectToString(o1);
                String s2 = normalizeObjectToString(o2);

                Assertions.assertEquals(s1, s2, "Row " + i + ", Field " + fi + " mismatch");
            }
        }
    }

    private String normalizeObjectToString(Object o) {
        if (o == null) {
            return "null";
        }
        if (o instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) o;
            Map<Object, Object> sortedMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                sortedMap.put(entry.getKey().toString(), normalizeObjectToString(entry.getValue()));
            }
            return sortedMap.toString();
        }

        if (o.getClass().isArray()) {
            if (o instanceof Object[]) {
                Object[] arr = (Object[]) o;
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < arr.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    } else {
                        sb.append(normalizeObjectToString(arr[i]));
                    }
                }
                sb.append("]");
                return sb.toString();
            }
        }

        return o.toString();
    }

    @Test
    void testListDatabases() {
        List<String> databases = catalog.listDatabases();
        assertThat(databases).isNotNull();
        assertThat(databases).contains(TEST_DB);
    }

    @Test
    void testDatabaseExists() {
        assertThat(catalog.databaseExists("default")).isTrue();
        assertThat(catalog.databaseExists("non_existing_db")).isFalse();
        assertThatThrownBy(() -> catalog.listTables("non_existing_db"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`.`%s`",
                                                TEST_CATALOG_NAME,
                                                catalog.getDefaultDatabase(),
                                                testTable.getTableName()))
                                .execute()
                                .collect());
        assertRowsEquals(testData, results);
    }

    @Test
    void testGetTable() throws TableNotExistException {
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(TEST_DB, testTable.getTableName()));
        clickhouseTableSchemaEquals(table.getUnresolvedSchema(), testTable.getTableSchema());
    }

    private void clickhouseTableSchemaEquals(Schema expected, Schema actual) {
        for (int i = 0; i < expected.getColumns().size(); i++) {
            Assertions.assertEquals(expected.getColumns().get(i), actual.getColumns().get(i));
        }
    }

    @Test
    void testSelectToInsert() throws Exception {

        String sql =
                String.format(
                        "insert into `%s` select * from `%s`",
                        testSinkTable.getTableName(), testTable.getTableName());
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s", testSinkTable.getTableName()))
                                .execute()
                                .collect());
        assertRowsEquals(testData, results);
    }
}
