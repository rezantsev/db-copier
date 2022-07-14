package com.database.copier.core.descriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class TableDescriptor {
	private List<String> columns = new ArrayList<>();
	private Map<String, String> columnTypes = new HashMap<>();
	private List<ForeignKey> foreignKeysToParents = new ArrayList<>();
	private List<ForeignKey> foreignKeysToChildren = new ArrayList<>();

	public void addColumn(String column, String type) {
		columns.add(column);
		columnTypes.put(column, type);
	}

	public void addForeignKeyToParents(ForeignKey foreignKey) {
		foreignKeysToParents.add(foreignKey);
	}

	public void addForeignKeyToChildren(ForeignKey foreignKey) {
		foreignKeysToChildren.add(foreignKey);
	}

	public ForeignKey getForeignKeyToParent(String key) {
		for (ForeignKey foreignKey : foreignKeysToParents) {
			if (foreignKey.getColumn().equalsIgnoreCase(key)) {
				return foreignKey;
			}
		}
		return null;
	}

	public List<ForeignKey> getForeignKeysToChildren(String key) {
		List<ForeignKey> links = new ArrayList<>();
		for (ForeignKey foreignKey : foreignKeysToChildren) {
			if (foreignKey.getReferencedColumn().equalsIgnoreCase(key)) {
				links.add(foreignKey);
			}
		}
		return links;
	}

	public List<ForeignKey> getLinksToParentTables(Map<String, Object> values) {
		List<ForeignKey> links = new ArrayList<>();
		for (Map.Entry<String, Object> column : values.entrySet()) {
			if (column.getValue() != null) {
				ForeignKey foreignKey = getForeignKeyToParent(column.getKey());
				if (foreignKey != null) {
					links.add(foreignKey);
				}
			}
		}
		return links;
	}

	public List<ForeignKey> getLinksToChildTables(Map<String, Object> values) {
		List<ForeignKey> links = new ArrayList<>();
		for (Map.Entry<String, Object> column : values.entrySet()) {
			if (column.getValue() != null) {
				List<ForeignKey> foreignKeys = getForeignKeysToChildren(column.getKey());
				links.addAll(foreignKeys);
			}
		}
		return links;
	}

	public boolean excludeChildForeignKey(String foreignKeyName) {
		for (ForeignKey foreignKey : foreignKeysToChildren) {
			if (foreignKey.getConstraintName().equalsIgnoreCase(foreignKeyName)) {
				return true;
			}
		}
		return false;
	}
}
