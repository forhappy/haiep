/**
 * forhappy
 * Date: 3:29:46 PM, Apr 9, 2012
 */
package cn.iie.haiep.hbase.admin;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.iie.haiep.rdbms.export.SQLExporter;

/**
 * @author forhappy
 *
 */
public class HaiepAdminMTTest {
	
	public static void log(String msg) {
		System.out.println(getTimeStamp() + " " + msg);
	}

	private static String getTimeStamp() {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return f.format(new Date());
	}

	/**
	 * @param args
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		String catalog = "eid";
		
		ExecutorService executor;
		int cpuCoreNumber;
		
		cpuCoreNumber = Runtime.getRuntime().availableProcessors();
		executor = Executors.newFixedThreadPool(cpuCoreNumber * 4);
		
		HaiepAdmin haiepAdmin = new HaiepAdmin(username, password, url, catalog);
		/**
		 * create schema.
		 */
		log("creating Scheam...");
		haiepAdmin.initialize();
		log("Scheam created...");
		
		log("Initializing SQLExporter...");
		SQLExporter sqlExporter = 
			new SQLExporter(url, username, password, catalog);
		log("SQLExporter Initialized...");
		
		log("Migration Launching...");
		while(sqlExporter.hasNextDataTable()) {
			Entry entry = sqlExporter.next();
			String tableName = (String) entry.getKey();
			List<Map<String, Object>> list = (List<Map<String, Object>>) entry.getValue();
			MigrateTableMT migration = new MigrateTableMT(tableName, list);
			executor.execute(migration);
		}
		
		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {
		}
		log("Migration Finished...");
	}
}
