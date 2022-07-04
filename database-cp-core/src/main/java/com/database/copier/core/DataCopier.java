package com.database.copier.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.database.copier.core.descriptor.ForeignKey;
import com.database.copier.core.descriptor.TableDescriptor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataCopier {
	private Connection connection;

	public DataCopier(Connection connection) {
		this.connection = connection;
	}

	public void copyRow(String schema, String table, String column, String value, Set<String> includeForeignKeys) throws SQLException {
		copyRow(schema, table, column, value, null, false, new DataCopierContext(includeForeignKeys));
	}

	private void copyRow(String schema, String table, String column, String value, ForeignKey foreignKey, boolean isChild, DataCopierContext context) throws SQLException {
		TableDescriptor tableDescriptor = getTableDescriptor(schema, table);
		ResultSet resultSet = selectByColumnValue(schema, table, column, value);
		while (resultSet.next()) {
			Map<String, Object> values = getValues(resultSet, tableDescriptor.getColumns());
			if (context.isVisited(tableDescriptor.getColumns(), values)) {
				continue;
			}
			context.setVisited(tableDescriptor.getColumns(), values);
			processParentTables(tableDescriptor, values, context);
			processTable(tableDescriptor.getColumns(), values, schema, table, foreignKey, isChild);
			processChildTables(tableDescriptor, values, context);
		}
	}

	private void processParentTables(TableDescriptor tableDescriptor, Map<String, Object> values, DataCopierContext context) throws SQLException {
		List<ForeignKey> linksToParentTables = tableDescriptor.getLinksToParentTables(values);
		if (!linksToParentTables.isEmpty()) {
			for (ForeignKey foreignKey : linksToParentTables) {
				String parentId = String.valueOf(values.get(foreignKey.getColumn()));
				copyRow(foreignKey.getReferencedSchema(), foreignKey.getReferencedTable(), foreignKey.getReferencedColumn(), parentId, foreignKey, false, context);
			}
		}
	}

	private void processTable(List<String> columns, Map<String, Object> values, String schema, String table, ForeignKey foreignKey, boolean isChild) throws SQLException {
		StringBuilder sb = new StringBuilder("INSERT INTO ").append(schema).append(".").append(table).append(" (");
		String columnNames = columns.stream().collect(Collectors.joining(", "));
		sb.append(columnNames);
		sb.append(") VALUES (").append(joinValues(columns, values)).append(");");
		if (foreignKey != null) {
			log.debug((isChild ? "Child FK: " : "Parent FK: ") + foreignKey.getConstraintName());
		}
		log.debug(sb.toString());
	}

	private void processChildTables(TableDescriptor tableDescriptor, Map<String, Object> values, DataCopierContext context) throws SQLException {
		List<ForeignKey> linksToChildTables = tableDescriptor.getLinksToChildTables(values);
		if (!linksToChildTables.isEmpty()) {
			for (ForeignKey foreignKey : linksToChildTables) {
				if (!context.includeForeignKey(foreignKey.getConstraintName())) {
					continue;
				}
				String childId = String.valueOf(values.get(foreignKey.getReferencedColumn()));
				copyRow(foreignKey.getSchema(), foreignKey.getTable(), foreignKey.getColumn(), childId, foreignKey, true, context);
			}
		}
	}

	private ResultSet selectByColumnValue(String schema, String table, String column, String value) throws SQLException {
		String sql = "select * from " + schema + "." + table + " where " + column + " = " + value;
		return connection.createStatement().executeQuery(sql);
	}

	private TableDescriptor getTableDescriptor(String schema, String table) throws SQLException {
		TableDescriptor tableDescriptor = new TableDescriptor();
		ResultSet resultSet = connection.createStatement().executeQuery("describe " + schema + "." + table);
		while (resultSet.next()) {
			String name = resultSet.getString("Field");
			String type = resultSet.getString("Type");
			tableDescriptor.addColumn(name, type);
		}
		String foreignKeysToParentSql = "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + table
		        + "' and REFERENCED_TABLE_NAME IS NOT NULL";
		resultSet = connection.createStatement().executeQuery(foreignKeysToParentSql);
		while (resultSet.next()) {
			tableDescriptor.addForeignKeyToParents(fetchForeignKey(resultSet));
		}
		String foreignKeysToChildrenSql = "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '" + schema + "' AND REFERENCED_TABLE_NAME = '" + table + "'";
		resultSet = connection.createStatement().executeQuery(foreignKeysToChildrenSql);
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
			sb.append("'").append(values.get(column)).append("'");
		}
		return sb.toString();
	}
}
