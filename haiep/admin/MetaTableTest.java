package cn.iie.haiep.hbase.admin;

public class MetaTableTest {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String username = "SYSDBA";
		String password = "SYSDBA";
		String url = "jdbc:dm://192.168.1.225:12345/EMS";
		String catalog = "EMS";
		
		MetaTable metatable = new MetaTable();
		metatable.initialize();
		metatable.importMetadataFromRDBMS(url, username, password, catalog);
	}
}
