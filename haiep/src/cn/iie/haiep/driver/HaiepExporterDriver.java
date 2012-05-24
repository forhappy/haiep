package cn.iie.haiep.driver;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.iie.haiep.hbase.admin.HaiepExporter;

public class HaiepExporterDriver {
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
//		String username = "root";
//		String password = "forhappy";
//		String url = "jdbc:mysql://localhost/eid";
//		String catalog = "eid";
		
		String username = "autonym";
		String password = "111111";
		String url = "jdbc:oracle:thin:@192.168.1.203:1521:db";
		String catalog = "";
		
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
//		haiepExporter.export();
		haiepExporter.export(30000);
		log("Migration Finished...");
		
	}
}

