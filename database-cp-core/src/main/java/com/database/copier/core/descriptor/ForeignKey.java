package com.database.copier.core.descriptor;

import lombok.Data;

@Data
public class ForeignKey {
	private String constraintName;
	private String column;
	private String table;
	private String schema;
	private String referencedSchema;
	private String referencedTable;
	private String referencedColumn;
}
