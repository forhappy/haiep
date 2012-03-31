package cn.iie.haiep.rdbms.metadata;

/**
 * @author forhappy
 * Date: 2012-3-26, 10:56 PM.
 */

public class DatabaseTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		
		Database database = new Database(url, username, password, "eid");
		database.fillTableLists();
	}

}
