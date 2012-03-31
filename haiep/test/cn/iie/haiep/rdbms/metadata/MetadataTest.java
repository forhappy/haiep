package cn.iie.haiep.rdbms.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MetadataTest {
	public static void main(String args[]) {
		String url = "jdbc:mysql://localhost/eid";
		Connection con;
		DatabaseMetaData dbmd;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (java.lang.ClassNotFoundException e) {
			System.err.print("ClassNotFoundException: ");
			System.err.println(e.getMessage());
		}
		try {
			con = DriverManager.getConnection(url, "root", "forhappy");
			dbmd = con.getMetaData();
			ResultSet rs = dbmd.getPrimaryKeys(null, null, "eid_verification");
			ResultSetMetaData rsmd = rs.getMetaData();

			// Display the result set data.
			int cols = rsmd.getColumnCount();
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					System.out.println(rs.getString(i));
				}
			}
//			executeGetTables(con);
//			executeGetSchemas(con);
			rs.close();

			con.close();
		} catch (SQLException ex) {
			System.err.println("SQLException: " + ex.getMessage());
		}
	}

	public static void executeGetTables(Connection con) {
		try {
			DatabaseMetaData dbmd = con.getMetaData();
			ResultSet rs = dbmd.getTables(null, null, null, null);
			ResultSetMetaData rsmd = rs.getMetaData();

			// Display the result set data.
			int cols = rsmd.getColumnCount();
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					System.out.println(rs.getString(i));
				}
			}
			rs.close();
		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void executeGetSchemas(Connection con) {
		try {
			DatabaseMetaData dbmd = con.getMetaData();
			ResultSet rs = dbmd.getSchemas();
			ResultSetMetaData rsmd = rs.getMetaData();

			// Display the result set data.
			int cols = rsmd.getColumnCount();
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					System.out.println(rs.getString(i));
				}
			}
			rs.close();
		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
