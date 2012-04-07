/**
 * forhappy
 * Date: 10:39:45 AM, Apr 7, 2012
 */
package cn.iie.haiep.rdbms.export;


public class SQLExporterTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		String catalog = "eid";
		
		SQLExporter sqlExporter = new SQLExporter(url, username, password, catalog);
		

	}

}
