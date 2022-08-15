package com.database.copier.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.database.copier.core.descriptor.ForeignKey;
import com.database.copier.core.descriptor.TableDescriptor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataCopier {
    private Connection connection;

    public DataCopier(Connection connection) {
        this.connection = connection;
    }

    public void copyRow(String schema, String table, String column, String value, Set<String> includeForeignKeys, StringBuilder output) throws SQLException {
        copyRow(schema, table, column, value, output, new DataCopierContext(includeForeignKeys));
    }

    private void copyRow(String schema, String table, String column, String value, StringBuilder output, DataCopierContext context) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            TableDescriptor tableDescriptor = getTableDescriptor(statement, schema, table);
            String sql = "select * from " + schema + "." + table + " where " + column + " = " + value;
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                Map<String, Object> values = getValues(resultSet, tableDescriptor.getColumns());
                if (context.isVisited(tableDescriptor.getColumns(), values)) {
                    continue;
                }
                context.setVisited(tableDescriptor.getColumns(), values);
                processParentTables(tableDescriptor, values, output, context);
                processTable(column, tableDescriptor.getColumns(), values, schema, table, output);
                processChildTables(tableDescriptor, values, output, context);
            }
        }
    }

    private void processParentTables(TableDescriptor tableDescriptor, Map<String, Object> values, StringBuilder output, DataCopierContext context)
            throws SQLException {
        List<ForeignKey> linksToParentTables = tableDescriptor.getLinksToParentTables(values);
        if (!linksToParentTables.isEmpty()) {
            for (ForeignKey foreignKey : linksToParentTables) {
                String parentId = String.valueOf(values.get(foreignKey.getColumn()));
                copyRow(foreignKey.getReferencedSchema(), foreignKey.getReferencedTable(), foreignKey.getReferencedColumn(), parentId, output, context);
            }
        }
    }

    private void processTable(String idColumn, List<String> columns, Map<String, Object> values, String schema, String table, StringBuilder sb) {
        sb.append("INSERT INTO ").append(schema).append(".").append(table).append(" (").append(joinColumns(columns))
                .append(") VALUES (").append(joinValues(columns, values)).append(") ON DUPLICATE KEY UPDATE ").append(idColumn).append(" = ").append(idColumn)
                .append(";\n");
        log.info(sb.toString());
    }

    private static String joinColumns(List<String> columns) {
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append('`').append(column).append('`');
        }
        return sb.toString();
    }

    private void processChildTables(TableDescriptor tableDescriptor, Map<String, Object> values, StringBuilder output, DataCopierContext context)
            throws SQLException {
        List<ForeignKey> linksToChildTables = tableDescriptor.getLinksToChildTables(values);
        if (!linksToChildTables.isEmpty()) {
            for (ForeignKey foreignKey : linksToChildTables) {
                if (!context.includeForeignKey(foreignKey.getConstraintName())) {
                    continue;
                }
                String childId = String.valueOf(values.get(foreignKey.getReferencedColumn()));
                copyRow(foreignKey.getSchema(), foreignKey.getTable(), foreignKey.getColumn(), childId, output, context);
            }
        }
    }

    private TableDescriptor getTableDescriptor(Statement statement, String schema, String table) throws SQLException {
        TableDescriptor tableDescriptor = new TableDescriptor();
        ResultSet resultSet = statement.executeQuery("describe " + schema + "." + table);
        while (resultSet.next()) {
            String name = resultSet.getString("Field");
            String type = resultSet.getString("Type");
            tableDescriptor.addColumn(name, type);
        }
        String foreignKeysToParentSql = "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + table
                + "' and REFERENCED_TABLE_NAME IS NOT NULL";
        resultSet = statement.executeQuery(foreignKeysToParentSql);
        while (resultSet.next()) {
            tableDescriptor.addForeignKeyToParents(fetchForeignKey(resultSet));
        }
        String foreignKeysToChildrenSql = "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '" + schema
                + "' AND REFERENCED_TABLE_NAME = '" + table + "'";
        resultSet = statement.executeQuery(foreignKeysToChildrenSql);
        while (resultSet.next()) {
            tableDescriptor.addForeignKeyToChildren(fetchForeignKey(resultSet));
        }
        return tableDescriptor;
    }

    private ForeignKey fetchForeignKey(ResultSet resultSet) throws SQLException {
        ForeignKey foreignKey = new ForeignKey();
        foreignKey.setConstraintName(resultSet.getString("CONSTRAINT_NAME"));
        foreignKey.setColumn(resultSet.getString("COLUMN_NAME"));
        foreignKey.setTable(resultSet.getString("TABLE_NAME"));
        foreignKey.setSchema(resultSet.getString("TABLE_SCHEMA"));
        foreignKey.setReferencedSchema(resultSet.getString("REFERENCED_TABLE_SCHEMA"));
        foreignKey.setReferencedTable(resultSet.getString("REFERENCED_TABLE_NAME"));
        foreignKey.setReferencedColumn(resultSet.getString("REFERENCED_COLUMN_NAME"));
        return foreignKey;
    }

    private Map<String, Object> getValues(ResultSet resultSet, List<String> columns) throws SQLException {
        Map<String, Object> values = new HashMap<>();
        for (String column : columns) {
            Object value = resultSet.getObject(column);
            values.put(column, value);
        }
        return values;
    }

    private String joinValues(List<String> columns, Map<String, Object> values) {
        StringBuilder sb = new StringBuilder();
        for (String column : columns) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            Object value = values.get(column);
            if (value != null) {
                if (value instanceof Boolean) {
                    sb.append(value);
                } else if (value instanceof String) {
                    value = ((String) value).replace("'", "''");
                    sb.append("'").append(value).append("'");
                } else {
                    sb.append("'").append(value).append("'");
                }
            } else {
                sb.append("NULL");
            }
        }
        return sb.toString();
    }
}
