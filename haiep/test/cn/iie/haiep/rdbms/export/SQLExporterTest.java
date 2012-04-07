/**
 * forhappy
 * Date: 10:39:45 AM, Apr 7, 2012
 */
package cn.iie.haiep.rdbms.export;

import java.util.List;
import java.util.Map;


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
		int counter = 0;
		
		while(sqlExporter.hasNextDataTable()) {
			System.out.println("========" + counter + "=======");
			List<Map<String, Object>> list = sqlExporter.nextDataTable();
			for (int i = 0; i < list.size(); i++) {
				Map<String, Object> map = list.get(i);
				for (Map.Entry<String, Object> m : map.entrySet()) {
					System.out.println("Key: " + m.getKey());
					System.out.println("Value: " + m.getValue());
				}
			}
			counter ++;
			
		}
		

	}

}
