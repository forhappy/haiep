package cn.iie.haiep.rdbms.driver;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author forhappy
 * Date: 2012-2-28, 10:45 PM.
 */


public class JDBCDriverTest {
	private static final Logger logger = LoggerFactory.getLogger(JDBCDriverTest.class);
	
	public static void main(String[] args) {
		Connection conn = null;
		try {
			String username = "root";
			String password = "forhappy";
			String url = "jdbc:mysql://localhost/eid";

			// RDBMSDriverManager driver = new RDBMSDriverManager(url, userName, passwd);
			// conn = driver.createConnection();
//			RDBMSDriverManager driver = new RDBMSDriverManager();
			java.util.Properties prop = new java.util.Properties();
			prop.put("charSet", "utf8");
			prop.put("user", username);
			prop.put("password", password);
			conn = RDBMSDriverManager.createConnection(url, prop);
			logger.info("Database connection established");
			
			/**
			 * Get metadata.
			 */
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rscat = dbmd.getCatalogs();
			String catalog = null;
			String columnName = null;
			String columnType = null;
			String tableSchem = null;
			int columnSize = 0;
			short dataType = 0;
			/**
			 * TABLE_SCHEM, 
			 */
			
			ResultSet rscols = dbmd.getColumns(null, null, null, null);
			ResultSetMetaData rsmd = rscols.getMetaData();

		      // Display the result set data.
		    int cols = rsmd.getColumnCount();
		    System.out.println(cols);
			while (rscols.next()) {
				for (int i = 1; i <= cols; i++) {
					 System.out.println(rscols.getString(i));
				} 
				System.out.println();
			}
			 
		/*	while (rscat.next()) {
				catalog = rscat.getString(1);
				if (catalog.equalsIgnoreCase("eid")) {
					ResultSet rscols = dbmd.getColumns(null, null, null, null);
					ResultSetMetaData rsmd = rscols.getMetaData();

				      // Display the result set data.
				    int cols = rsmd.getColumnCount();
				    System.out.println(cols);
					while (rscols.next()) {
						for (int i = 1; i <= cols; i++) {
							 System.out.println(rscols.getString(i));
						} 
						System.out.println();
						tableSchem = rscols.getString("TABLE_SCHEM");
						System.out.println("TABLE_SCHEM: " + tableSchem);
						columnName = rscols.getString("COLUMN_NAME");
						System.out.println("COLUMN_NAME: " + columnName);
						columnType = rscols.getString("TYPE_NAME");
						System.out.println("TYPE_NAME: " + columnType);
						columnSize = rscols.getInt("COLUMN_SIZE");
						System.out.println("COLUMN_SIZE: " + columnSize);
						dataType = rscols.getShort("DATA_TYPE");
						System.out.println("DATA_TYPE: " + dataType);
						System.out.println();
//						int datasize = rscols.getInt("COLUMN_SIZE");
//						int digits = rscols.getInt("DECIMAL_DIGITS");
//						int nullable = rscols.getInt("NULLABLE");
//						System.out.println(columnName + " " + columnType + " " + datasize + " " + digits + " " + nullable); 
					}
					
				}
			}*/
			
			/**
			 * Query
			 */
			String query = "select * from eid_verification";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			
//			while (rs.next()) {
//				System.out.println(rs.getString(2));
//			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			if (conn != null) {
				try {
					RDBMSDriverManager.close(conn);
					logger.info("Database connection terminated");
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}
}
