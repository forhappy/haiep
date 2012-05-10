/**
 * forhappy
 * Date: 7:79:14 PM, May 9, 2012
 */
package cn.iie.haiep.rdbms.export;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.PreparedStatement;

import cn.iie.haiep.rdbms.driver.RDBMSDriverManager;
import cn.iie.haiep.rdbms.metadata.Column;
import cn.iie.haiep.rdbms.metadata.Database;
import cn.iie.haiep.rdbms.metadata.RowSchema;
import cn.iie.haiep.rdbms.metadata.Table;

public class SQLMetaExporter {
	
	/**
	 * @return the metadataMap
	 */
	public Map<String, Map<String, String>> getMetadataMap() {
		return metadataMap;
	}

	/**
	 * @return the resultsetMap
	 */
	public Map<String, ResultSet> getResultsetMap() {
		return resultsetMap;
	}

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
	public SQLMetaExporter(String url, String user, String password, String catalog) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		this.catalog = catalog;
		try {
			logger.info("Initializing SQLMetaExporter...");
			driver = new RDBMSDriverManager();
			conn = RDBMSDriverManager.createConnection(this.url, this.user,
					this.password);
//			stmt = conn.createStatement();
			/**
			 * fill metadata.
			 */
			fillMetadataMap();
			
			/**
			 * generate fileds.
			 */
			fillStatements();
			
			
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
		ResultSet rs = null;
		String query = "select * from " + tableName;
		try {
			pstmt = (PreparedStatement) conn.prepareStatement(query);
//			pstmt.setString(1, tableName);
			rs = pstmt.executeQuery();
			preStmtsMap.put(tableName, pstmt);
			resultsetMap.put(tableName, rs);
		} catch (SQLException e) {
			RDBMSDriverManager.closeQuietly(pstmt);
			RDBMSDriverManager.closeQuietly(rs);
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	private void fillStatements() {
		for (Map.Entry<String, Map<String, String>> m : metadataMap.entrySet()) {
			String tableName = m.getKey();
			generateFields(tableName);
		}
	}
	
	/**
	 * fill metadata, the class Database database. 
	 */
	private void fillMetadataMap() {
		database = new Database(url, user, password, catalog);
		database.fillTableLists();
		
		while(database.hasNextTable()) {
			Table tbl = database.next();
			String tableName = tbl.getTableName();
			RowSchema rowSchema = tbl.getRowSchema();
			Map<String, String> columnMap = new HashMap<String, String>();
			while (rowSchema.hasNextColumn()) {
				Column column = rowSchema.next();
				String columnName = column.getColumnName();
				String typeName = column.getTypeName();
				columnMap.put(columnName, typeName);
				
			}
			/**
			 * put table name and columnMap.
			 */
			metadataMap.put(tableName, columnMap);
		}
		iterMetaMap = metadataMap.entrySet().iterator();
	}
	
	/**
	 * import data into memory at first.
	 * @param tableName
	 * @return a list structure that contains all data records in the RDBMS.
	 */
	public List<Map<String, Object>> buildTableInMemory(String tableName) {
		Map<String, String> columnMetadataMap = metadataMap.get(tableName);
		List<Map<String, Object>> listData = new ArrayList<Map<String, Object>>();
		try {
			ResultSet rs = resultsetMap.get(tableName);
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();

				for (Map.Entry<String, String> columnMap : columnMetadataMap
						.entrySet()) {
					String columnLabel = columnMap.getKey();
					Object object = rs.getObject(columnLabel);
					map.put(columnLabel, object);
				}
				listData.add(map);
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return listData;
	}
	
	public Boolean hasNextTable() {
		return iterMetaMap.hasNext();
	}
	
	@SuppressWarnings("rawtypes")
	public String nextTable() {
		Entry thisEntry = (Entry) iterMetaMap.next();
		return (String) thisEntry.getKey();
	}
	
	/**
	 * a Map that contains the database metadata.
	 */
	private Map<String, Map<String, String>> metadataMap = 
		new HashMap<String, Map<String, String>>();
	
	@SuppressWarnings("rawtypes")
	private Iterator iterMetaMap = metadataMap.entrySet().iterator();
	
	/**
	 * Private fields.
	 */
	private String url = null;
	private String user = null;
	private String password = null;
	private String catalog = null;

	private RDBMSDriverManager driver = null;
	private Connection conn = null;
	/*
	 * preStmtsMap indicates and table name and its respective statements in SQL.  
	 */
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
