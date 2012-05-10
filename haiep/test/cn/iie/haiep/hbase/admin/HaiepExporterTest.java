package cn.iie.haiep.hbase.admin;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HaiepExporterTest {
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
	public static void main(String[] args) {
		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		String catalog = "eid";
		
		HaiepExporter haiepExporter = new HaiepExporter(username, password, url, catalog);
		
		log("creating Scheam...");
		haiepExporter.initialize();
		log("Scheam created...");
		
		/**
		 * migrate data put by put.
		 */
//		log("Migration Launching...");
//		haiepAdmin.migrateData();
//		log("Migration Finished...");
		
		/**
		 * migrate data by list puts.
		 */
		log("Migration Launching...");
		haiepExporter.export();
		log("Migration Finished...");
		
	}
}
