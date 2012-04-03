package cn.iie.haiep.rdbms.metadata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.iie.haiep.rdbms.driver.RDBMSDriverManager;

/**
 * @author forhappy
 * Date: 2012-3-26, 10:56 PM.
 */


public class Database {
	
	public Database(String catalog) {
		super();
		this.catalog = catalog;
		listTables = new ArrayList<Table>();
	}

	/**
	 * constructor without any parameters.
	 */
	public Database() {
		super();
		listTables = new ArrayList<Table>();
	}

	/**
	 * constructor with table list.
	 * @param listTables
	 */
	public Database(List<Table> listTables) {
		super();
		this.listTables = listTables;
		
	}
	
	
	/**
	 * construct a database instance using url, user and password.
	 * @param url a database url of the form "jdbc:subprotocol:subname".
	 * @param user the database user on whose behalf the connection is being made.
	 * @param password the user's password.
	 */
	public Database(String url, String user, String password) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		this.driver = new RDBMSDriverManager(url, user, password);
		try {
			this.conn = this.driver.createConnection();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		listTables = new ArrayList<Table>();
	}
	
	public Database(String url, String user, String password, String catalog) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		this.catalog = catalog;
		this.driver = new RDBMSDriverManager(url, user, password);
		try {
			this.conn = this.driver.createConnection();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		listTables = new ArrayList<Table>();
	}

	public void createConnection() {
		if (url != null) {
			if (user != null) {
				if (password != null) {
					this.driver = new RDBMSDriverManager(url, user, password);
					try {
						this.conn = this.driver.createConnection();
					} catch (SQLException e) {
						logger.error(e.getMessage());
						e.printStackTrace();
					}
				} else {
					logger.error("password cannot be null, you should set it at first.");
				}
			} else {
				logger.error("user cannot be null, you should set it at first.");
			}
		} else {
			logger.error("url cannot be null, you should set it at first.");
		}
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

	/**
	 * @return the listTables
	 */
	public List<Table> getListTables() {
		return listTables;
	}


	/**
	 * @param listTables the listTables to set
	 */
	public void setListTables(List<Table> listTables) {
		this.listTables = listTables;
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


	public void addTable(Table table) {
		listTables.add(table);
	}
	
	
	public void addTables(List<Table> tables) {
		listTables.addAll(tables);
	}
	
	
	public void deleteTable(Table table) {
		listTables.remove(table);
	}
	
	
	public void deleteTables(List<Table> tables) {
		listTables.removeAll(tables);
	}
	
	public void fillTableLists() {
		
		/**
		 * get all tables in the catalog.
		 */
		generateTables();
		
		try {
			while (nextTable()) {
				try {
					Table table = new Table();
					table.setTableName(rsTables.getString("TABLE_NAME"));
					table.setTableCatalog(rsTables.getString("TABLE_CAT"));
					table.setTableSchema(rsTables.getString("TABLE_SCHEM"));
					table.setTableType(rsTables.getString("TABLE_TYPE"));
					table.setRemarks(rsTables.getString("REMARKS"));
					
					/**
					 * here in after metadata about the table only available in jdbc 3.0
					 * and not support in mysql-connector-java.3.1.14 
					 */
//					table.setTypeCatalog(rsTables.getString("TYPE_CAT"));
//					table.setTypeName(rsTables.getString("TYPE_NAME"));
//					table.setTypeSchema(rsTables.getString("TYPE_SCHEMA"));
					
					table.fillRowSchema(conn);
					/**
					 * add table listTables.
					 */
					addTable(table);
				} catch (SQLException e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			iterTable = listTables.iterator();
			this.close();
		}
	}
	
	public void close() {
		try {
			RDBMSDriverManager.close(conn);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void closeQuietly() {
		RDBMSDriverManager.closeQuietly(conn);
	}
	
	/**
	 * Find the table description specified by name, iff there exits such an table, 
	 * then return it; otherwise return null.
	 * @param tableName
	 * @return Table description specified by tableName, iff there exits such an table,
	 * then return it; otherwise return null.
	 */
	public Table findTableByName(String tableName) {
		Iterator iter = listTables.iterator();
		while (iter.hasNext()) {
			Table table = (Table) iter.next();
			if (table.getTableName().equalsIgnoreCase(tableName)) {
				return table;
			}
		}
		return null;
	}
	
	public RowSchema findRowSchemaByTableName(String tableName) {
		Iterator iter = listTables.iterator();
		while (iter.hasNext()) {
			Table table = (Table) iter.next();
			if (table.getTableName().equalsIgnoreCase(tableName)) {
				RowSchema rowSchema = table.getRowSchema();
			}
		}
		return null;
	}
	
	private void generateTables() {
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			rsTables = dbmd.getTables(catalog, null, null, null);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	private Boolean nextTable() throws IOException {
		try {
			return rsTables.next();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
	}
	
	public Boolean hasNextTable() {
		return iterTable.hasNext();
	}
	
	public Table next() {
		return (Table) iterTable.next();
	}
	
	private Iterator<Table> iterTable = null;
	
	private ResultSet rsTables = null;
	/**
	 * list of tables in a database
	 */
	private List<Table> listTables = null;
	
	/**
	 * a database url of the form "jdbc:subprotocol:subname".
	 */
	private String url = null;
	
	/**
	 * the database user on whose behalf the connection is being made.
	 */
	private String user = null;
	
	/**
	 * the user's password.
	 */
	private String password = null;
	
	/**
	 * database catalog.
	 */
	private String catalog = null;
	
	/**
	 * Connection to a database.
	 */
	Connection conn = null;
	
	/**
	 * RDBMS driver manager.
	 */
	RDBMSDriverManager driver = null;
	
	/**
	 * logger.
	 */
	private static final Logger logger = 
		LoggerFactory.getLogger(Database.class);
}