package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/** TableRow for ClickHouse. */
public class ClickHouseTableRow extends TableRow {
    public ClickHouseTableRow(String name, TableField[] fields) {
        super(name, fields);
    }

    private static final String CLICKHOUSE_DEFAULT_ENGINE = " ENGINE = Memory";

    @Override
    protected String getDeleteFromQuery() {
        return String.format("truncate table %s", getTableName());
    }

    @Override
    public String getCreateQuery() {
        String baseDDL = super.getCreateQuery();
        if (baseDDL.endsWith(";")) {
            baseDDL = baseDDL.substring(0, baseDDL.length() - 1);
        }

        return baseDDL + CLICKHOUSE_DEFAULT_ENGINE;
    }

    @Override
    protected JdbcResultSetBuilder<Row> getResultSetBuilder() {
        return (rs) -> {
            List<Row> result = new ArrayList<>();
            DataTypes.Field[] fields = getTableDataFields();
            while (rs.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    Object dbValue;
                    Class<?> conversionClass = fields[i].getDataType().getConversionClass();
                    if (conversionClass.equals(LocalTime.class)) {
                        dbValue = rs.getTime(i + 1);
                    } else if (conversionClass.equals(LocalDate.class)) {
                        dbValue = rs.getDate(i + 1);
                    } else if (conversionClass.equals(LocalDateTime.class)) {
                        Object value = rs.getObject(i + 1);
                        if (value instanceof OffsetDateTime) {
                            // https://github.com/ClickHouse/clickhouse-java/issues/2496
                            // The timestamp offset issue is expected to be resolved in version
                            // 0.9.5.
                            LocalDateTime dt =
                                    ((OffsetDateTime) value)
                                            .atZoneSameInstant(ZoneId.systemDefault())
                                            .toLocalDateTime();
                            Calendar c = Calendar.getInstance();
                            c.set(
                                    dt.getYear(),
                                    dt.getMonthValue() - 1,
                                    dt.getDayOfMonth(),
                                    dt.getHour(),
                                    dt.getMinute(),
                                    dt.getSecond());
                            Timestamp timestamp = new Timestamp(c.getTimeInMillis());
                            timestamp.setNanos(dt.getNano());
                            dbValue = timestamp;
                        } else {
                            dbValue = rs.getTimestamp(i + 1);
                        }
                    } else {
                        dbValue = rs.getObject(i + 1, conversionClass);
                    }
                    row.setField(i, getNullable(rs, dbValue));
                }
                result.add(row);
            }
            return result;
        };
    }

    @Override
    public void insertIntoTableValues(Connection conn, List<Row> values) throws SQLException {
        executeStatement(
                conn,
                getInsertIntoQuery(),
                (ps, row) -> {
                    DataTypes.Field[] fields = getTableDataFields();
                    for (int i = 0; i < row.getArity(); i++) {
                        DataType type = fields[i].getDataType();
                        LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
                        int dbType;
                        if (typeRoot == LogicalTypeRoot.MAP) {
                            dbType = Types.STRUCT;
                        } else {
                            dbType = JdbcTypeUtil.logicalTypeToSqlType(typeRoot);
                        }
                        if (row.getField(i) == null) {
                            ps.setNull(i + 1, dbType);
                        } else {
                            if (type.getConversionClass().equals(LocalTime.class)) {
                                Time time = Time.valueOf(row.<LocalTime>getFieldAs(i));
                                ps.setTime(i + 1, time);
                            } else if (type.getConversionClass().equals(LocalDate.class)) {
                                ps.setDate(i + 1, Date.valueOf(row.<LocalDate>getFieldAs(i)));
                            } else if (type.getConversionClass().equals(LocalDateTime.class)) {
                                ps.setTimestamp(
                                        i + 1, Timestamp.valueOf(row.<LocalDateTime>getFieldAs(i)));
                            } else {
                                ps.setObject(i + 1, row.getField(i));
                            }
                        }
                    }
                },
                values);
    }
}
