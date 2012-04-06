package cn.iie.haiep.hbase.admin;

public class HaiepAdminTest {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		String catalog = "eid";
		
		HaiepAdmin haiepAdmin = new HaiepAdmin(username, password, url, catalog);
		
		haiepAdmin.initialize();
		
	}
}
