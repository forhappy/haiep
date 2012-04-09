package cn.iie.haiep.hbase.admin;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HaiepAdminTest {
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
		
		HaiepAdmin haiepAdmin = new HaiepAdmin(username, password, url, catalog);
		
		log("creating Scheam...");
		haiepAdmin.initialize();
		log("Scheam created...");
		
		log("Migration Launching...");
		haiepAdmin.migrateData();
		log("Migration Finished...");
		
	}
}
