package cn.iie.haiep.hbase.admin;

public class MetaTableTest {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		String catalog = "eid";
		
		MetaTable metatable = new MetaTable();
		metatable.initialize();
		metatable.importMetadataFromRDBMS(url, username, password, catalog);
	}
}
