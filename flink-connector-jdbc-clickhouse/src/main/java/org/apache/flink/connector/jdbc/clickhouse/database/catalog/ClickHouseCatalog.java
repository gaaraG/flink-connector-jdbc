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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/** Catalog for ClickHouse. */
@Internal
public class ClickHouseCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCatalog.class);

    private final JdbcCatalogTypeMapper dialectTypeMapper;

    // ClickHouse internal databases to exclude
    private static final HashSet<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                    add("INFORMATION_SCHEMA");
                    add("system");
                }
            };

    @VisibleForTesting
    public ClickHouseCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                getBriefAuthProperties(username, pwd));
    }

    public ClickHouseCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectionProperties) {
        super(
                userClassLoader,
                catalogName,
                defaultDatabase,
                preprocessBaseUrl(baseUrl),
                connectionProperties);

        String driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");
        String databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "Database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
        this.dialectTypeMapper = new ClickHouseTypeMapper(databaseVersion, driverVersion);
    }

    /**
     * Preprocess the base URL to make it compatible with AbstractJdbcCatalog validation.
     *
     * <p>This method performs two main normalizations: 1. Standardizes the scheme separator to
     * "://" (e.g., converts "jdbc:clickhouse:host" to "jdbc:clickhouse://host"). 2. Strips the
     * database name and path if present (e.g., "jdbc:clickhouse://host:8123/default" ->
     * "jdbc:clickhouse://host:8123").
     *
     * <p>Supported inputs -> outputs: - jdbc:clickhouse://host:port -> jdbc:clickhouse://host:port
     * - jdbc:clickhouse:host:port -> jdbc:clickhouse://host:port - jdbc:ch://host:port ->
     * jdbc:ch://host:port - jdbc:ch:host:port -> jdbc:ch://host:port -
     * jdbc:clickhouse://host:port/db -> jdbc:clickhouse://host:port
     */
    private static String preprocessBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            return null;
        }

        String url = baseUrl.trim();

        if (url.startsWith("jdbc:clickhouse:") && !url.startsWith("jdbc:clickhouse://")) {
            url = url.replaceFirst("jdbc:clickhouse:", "jdbc:clickhouse://");
        } else if (url.startsWith("jdbc:ch:") && !url.startsWith("jdbc:ch://")) {
            url = url.replaceFirst("jdbc:ch:", "jdbc:ch://");
        }

        int doubleSlashIndex = url.indexOf("//");
        if (doubleSlashIndex != -1) {
            int pathSlashIndex = url.indexOf('/', doubleSlashIndex + 2);
            if (pathSlashIndex != -1) {
                return url.substring(0, pathSlashIndex);
            }
        }

        return url;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT `name` FROM `system`.`databases`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                getDatabaseUrl(databaseName),
                "SELECT `name` FROM `system`.`tables` WHERE `database` = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !extractColumnValuesBySQL(
                        baseUrl,
                        "SELECT `name` FROM `system`.`tables` " + "WHERE `database`=? and `name`=?",
                        1,
                        null,
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName())
                .isEmpty();
    }

    /**
     * Overridden getTable method.
     *
     * <p>We override this to manually handle table schema generation for ClickHouse. Standard JDBC
     * implementations often incorrectly map ClickHouse "Sorting Key" to JDBC "Primary Key". Since
     * Sorting Keys do not guarantee uniqueness, mapping them to Flink Primary Keys causes incorrect
     * behavior (e.g., in Upsert operations). By overriding this, we explicitly avoid setting the
     * Primary Key in the schema.
     */
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            String dbUrl = getDatabaseUrl(tablePath.getDatabaseName());

            try (Connection conn = DriverManager.getConnection(dbUrl, connectionProperties)) {
                // This is often more accurate and faster than DatabaseMetaData.getColumns for
                // ClickHouse.
                ResultSetMetaData rsMetaData = getResultSetMetaData(conn, tablePath);

                Schema.Builder schemaBuilder = Schema.newBuilder();
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    String columnName = rsMetaData.getColumnName(i);
                    DataType flinkType = fromJDBCType(tablePath, rsMetaData, i);

                    // Add column to schema.
                    // Note: We intentionally do NOT define a Primary Key here.
                    // ClickHouse Sorting Keys are not Unique Constraints.
                    schemaBuilder.column(columnName, flinkType);
                }
                // TODO Do we need to store some of the retrieved metadata or incorporate it into
                // the defined data?
                Map<String, String> ckTableProperties = getClickHouseTableMetadata(conn, tablePath);

                Map<String, String> props = new HashMap<>();
                props.put("connector", "jdbc");
                props.put("url", dbUrl);
                props.put("table-name", tablePath.getObjectName());
                props.put("username", getUsername());
                props.put("password", getPassword());

                // We pass empty list for partition keys because ClickHouse partitioning
                // is internal and handled via "scan.partition..." options, not Flink catalog
                // partitions.
                return CatalogTable.newBuilder()
                        .schema(schemaBuilder.build())
                        .comment(ckTableProperties.getOrDefault("comment", ""))
                        .partitionKeys(Collections.emptyList())
                        .options(props)
                        .build();
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to get table %s.", tablePath.getFullName()), e);
        }
    }

    private String getDatabaseVersion() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                return conn.getMetaData().getDatabaseProductVersion();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting ClickHouse version by %s.", defaultUrl),
                        e);
            }
        }
    }

    private String getDriverVersion() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                String driverVersion = conn.getMetaData().getDriverVersion();
                Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
                Matcher matcher = regexp.matcher(driverVersion);
                return matcher.find() ? matcher.group(0) : null;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format(
                                "Failed in getting ClickHouse driver version by %s.", defaultUrl),
                        e);
            }
        }
    }

    /** Converts ClickHouse type to Flink {@link DataType}. */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    /**
     * Executes a "SELECT * LIMIT 0" query to retrieve ResultSetMetaData. This ensures we get the
     * types exactly as the JDBC driver maps them for a result set.
     */
    private ResultSetMetaData getResultSetMetaData(Connection conn, ObjectPath tablePath)
            throws SQLException {
        String sql =
                String.format(
                        "SELECT * FROM %s.%s LIMIT 0",
                        quoteIdentifier(tablePath.getDatabaseName()),
                        quoteIdentifier(tablePath.getObjectName()));

        return conn.createStatement().executeQuery(sql).getMetaData();
    }

    /**
     * Queries the 'system.tables' table to retrieve ClickHouse specific metadata such as Engine,
     * Partition Key, and Sorting Key.
     */
    private Map<String, String> getClickHouseTableMetadata(Connection conn, ObjectPath tablePath) {
        Map<String, String> props = new HashMap<>();
        String sql =
                "SELECT engine, partition_key, sorting_key, comment "
                        + "FROM system.tables "
                        + "WHERE database = ? AND name = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getObjectName());
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    props.put("clickhouse.engine", rs.getString("engine"));
                    props.put("clickhouse.partition_key", rs.getString("partition_key"));
                    props.put("clickhouse.sorting_key", rs.getString("sorting_key"));

                    String comment = rs.getString("comment");
                    if (StringUtils.isNotBlank(comment)) {
                        props.put("comment", comment);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to retrieve extra metadata from ClickHouse system.tables for {}",
                    tablePath.getFullName(),
                    e);
        }
        return props;
    }

    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
