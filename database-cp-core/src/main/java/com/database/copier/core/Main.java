package com.database.copier.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class Main {

	public static void main(String[] args) throws SQLException {
		testWithRealDb();
	}
	
	private static void testWithDebugDb() throws SQLException {
		String url = "jdbc:mysql://localhost:3306";
		Connection connection = DriverManager.getConnection(url, "root", "root");
		
		Set<String> includeForeignKeys = new HashSet<>();
		includeForeignKeys.add("product_shop_fk");
		includeForeignKeys.add("product_characteristics_c_fk");
		
		
		DataCopier databaseDataCopier = new DataCopier(connection);
		//databaseDataCopier.copyRow("cp1", "shop", "id", "1", includeForeignKeys);
		databaseDataCopier.copyRow("cp1", "product", "id", "1", includeForeignKeys);
	}

	private static void testWithRealDb() throws SQLException {
		String url = "jdbc:mysql://localhost:3306";
		Connection connection = DriverManager.getConnection(url, "root", "natera123");
		
		Set<String> includeForeignKeys = new HashSet<>();
		includeForeignKeys.add("FK7B66DDECCB9103C3");
		includeForeignKeys.add("FK9863535DEE91E1D1");

		
		DataCopier databaseDataCopier = new DataCopier(connection);
		databaseDataCopier.copyRow("prodlims", "casefile", "id", "5107487", includeForeignKeys);
	}
}
