package com.database.copier.core;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataCopierContext {
	private Set<String> visitedEntries = new HashSet<>();
	private Set<String> includeForeignKeys;

	public DataCopierContext(Set<String> includeForeignKeys) {
		this.includeForeignKeys = includeForeignKeys;
	}

	public void setVisited(List<String> columns, Map<String, Object> values) {
		visitedEntries.add(getKey(columns, values));
	}

	public boolean isVisited(List<String> columns, Map<String, Object> values) {
		return visitedEntries.contains(getKey(columns, values));
	}

	private String getKey(List<String> columns, Map<String, Object> values) {
		StringBuilder sb = new StringBuilder();
		for (String column : columns) {
			sb.append(column).append("-").append(String.valueOf(values.get(column)));
		}
		return sb.toString();
	}
	
	public boolean includeForeignKey(String foreignKeyName) {
		return includeForeignKeys.contains(foreignKeyName);
	}
}
