package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
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

    private static final String CLICKHOUSE_ENGINE = " ENGINE = MergeTree() ORDER BY tuple()";

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

        return baseDDL + CLICKHOUSE_ENGINE;
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
}
