package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;

/**
 * TableRow for ClickHouse.
 */
public class ClickHouseTableRow extends TableRow {
    public ClickHouseTableRow(String name, TableField[] fields) {
        super(name, fields);
    }

    private static final String CLICKHOUSE_ENGINE = " ENGINE = MergeTree() ORDER BY (id) PRIMARY KEY (id)";

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
}
