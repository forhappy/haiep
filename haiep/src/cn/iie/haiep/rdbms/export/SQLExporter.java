/**
 * forhappy
 * Date: 9:16:14 PM, Apr 6, 2012
 */
package cn.iie.haiep.rdbms.export;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.PreparedStatement;

import cn.iie.haiep.rdbms.driver.RDBMSDriverManager;
import cn.iie.haiep.rdbms.metadata.Column;
import cn.iie.haiep.rdbms.metadata.Database;
import cn.iie.haiep.rdbms.metadata.RowSchema;
import cn.iie.haiep.rdbms.metadata.Table;

public class SQLExporter {
	
	/**
	 * Attempts to establish a connection to the given database URL.
	 * 
	 * @param url
	 *            a database url of the form "jdbc:subprotocol:subname".
	 * @param user
	 *            the database user on whose behalf the connection is being
	 *            made.
	 * @param password
	 *            the user's password.
	 * @return a connection to the URL.
	 * @throws SQLException
	 *             if a database access error occurs.
	 */
	public SQLExporter(String url, String user, String password, String catalog) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		this.catalog = catalog;
		try {
			logger.info("Initializing SQLExporter...");
			driver = new RDBMSDriverManager();
			conn = RDBMSDriverManager.createConnection(this.url, this.user,
					this.password);
//			stmt = conn.createStatement();
			/**
			 * fill metadata.
			 */
			fillMetadata();
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			RDBMSDriverManager.closeQuietly(conn);
//			RDBMSDriverManager.closeQuietly(stmt);
			e.printStackTrace();
		}
	}
	
	public void close() {
		RDBMSDriverManager.closeQuietly(conn);
		for (Map.Entry<String, PreparedStatement> m : preStmtsMap.entrySet()) {
			PreparedStatement pstmt = m.getValue();
			RDBMSDriverManager.closeQuietly(pstmt);
		}
		for (Map.Entry<String, ResultSet> m : resultsetMap.entrySet()) {
			ResultSet rs = m.getValue();
			RDBMSDriverManager.closeQuietly(rs);
		}
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the catalog
	 */
	public String getCatalog() {
		return catalog;
	}

	/**
	 * @param catalog the catalog to set
	 */
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	
	private void generateFields(String tableName) {
		PreparedStatement pstmt = null;
		String query = "select * from ?";
		try {
			pstmt = (PreparedStatement) conn.prepareStatement(query);
			pstmt.setString(1, tableName);
			pstmt.executeQuery();
			preStmtsMap.put(tableName, pstmt);
		} catch (SQLException e) {
			RDBMSDriverManager.closeQuietly(pstmt);
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
	}

	/**
	 * fill metadata, the class Database database. 
	 */
	private void fillMetadata() {
		database = new Database(url, user, password, catalog);
		database.fillTableLists();
		
		while(database.hasNextTable()) {
			Table tbl = database.next();
			String tableName = tbl.getTableName();
			RowSchema rowSchema = tbl.getRowSchema();
			while (rowSchema.hasNextColumn()) {
				Column column = rowSchema.next();
				Map<String, String> columnMap = new HashMap<String, String>();
				String columnName = column.getColumnName();
				String typeName = column.getTypeName();
				columnMap.put(columnName, typeName);
				
				/**
				 * put table name and columnMap.
				 */
				map.put(tableName, columnMap);
			}
		}
	}
	
	/**
	 * a Map that contains the database metadata.
	 */
	private Map<String, Map<String, String>> map = 
		new HashMap<String, Map<String, String>>();
	
	/**
	 * Private fields.
	 */
	private String url = null;
	private String user = null;
	private String password = null;
	private String catalog = null;

	private RDBMSDriverManager driver = null;
	private Connection conn = null;
	private Map<String, PreparedStatement> preStmtsMap = new HashMap<String, PreparedStatement>();
//	private Statement stmt = null;
	
	private Map<String, ResultSet> resultsetMap = new HashMap<String, ResultSet>();
	
	private Database database = null;
	
	/**
	 * logger.
	 */
	private static final Logger logger = 
		LoggerFactory.getLogger(SQLExporter.class);
}
