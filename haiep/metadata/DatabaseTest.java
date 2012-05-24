package cn.iie.haiep.rdbms.metadata;

import java.util.Iterator;
import java.util.List;

/**
 * @author forhappy
 * Date: 2012-3-26, 10:56 PM.
 */

public class DatabaseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		
		Database database = new Database(url, username, password, "eid");
		database.fillTableLists();
		Boolean hasNext = false;
		
/*		Iterator<Table> iter = database.getListTables().iterator();
		while (iter.hasNext()) {
			Table table = iter.next();
			System.out.println(table.getTableName());
		}*/
		
//		List<Table> tableList = database.getListTables();
//		for (Table table : tableList) {
//			System.out.println(table.getTableName());
//		}
		
		/**
		 * test hasNextTable, next, 
		 */
		while((hasNext = database.hasNextTable()) != false) {
			Table table = database.next();
			System.out.println("Catalog: " + table.getTableCatalog());
			System.out.println("Table Name: " + table.getTableName());
			System.out.println("Table Schema: " + table.getTableSchema());
			System.out.println("Table Type :" + table.getTableType());
			System.out.println("Remarks: " + table.getRemarks());
			/**
			 * test RowSchema.
			 */
			RowSchema rowSchema = table.getRowSchema();
			while(rowSchema.hasNextColumn()) {
				Column column = rowSchema.next();
				System.out.println("===========");
				System.out.println("Table Catalog: " + column.getTableCatalog());
				System.out.println("Table Name: " + column.getTableName());
				System.out.println("Table Schema: " + column.getTableSchema());
				System.out.println("Column Name: " + column.getColumnName());
				System.out.println("Data Type: " + column.getDataType());
				System.out.println("Type Name: " + column.getTypeName());
				System.out.println("Column Size: " + column.getColumnSize());
				System.out.println("Nullable: " + column.getIsNullable());
				System.out.println("Ordinal Position: " + column.getOrdinalPosition());
				System.out.println("Remarks: " + column.getRemarks());
				System.out.println("===========");
				System.out.println("");
			}
			System.out.println("************");
			System.out.println("");
			System.out.println("");
			
		}
		
		/**
		 * test findTableByName
		 */
/*		String tableName = "eid_id";
		Table tablex = database.findTableByName(tableName);
		System.out.println("Table Name: " + tablex.getTableName());*/
		
		
	}

}
