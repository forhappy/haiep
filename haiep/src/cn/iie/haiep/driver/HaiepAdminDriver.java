package cn.iie.haiep.driver;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.iie.haiep.hbase.admin.HaiepAdmin;

public class HaiepAdminDriver {
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
/*		String username = "root";
		String password = "forhappy";
		String url = "jdbc:mysql://localhost/eid";
		String catalog = "eid";*/
		
		String username = "SYSDBA";
		String password = "SYSDBA";
		String url = "jdbc:dm://192.168.1.225:12345/EMS";
		String catalog = "EMS";
		
		HaiepAdmin haiepAdmin = new HaiepAdmin(username, password, url, catalog);
		
		log("creating Scheam...");
		haiepAdmin.initialize();
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
		haiepAdmin.migrateDataByBatch();
		log("Migration Finished...");
		
	}
}
