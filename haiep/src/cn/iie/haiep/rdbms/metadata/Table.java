package cn.iie.haiep.rdbms.metadata;

import java.io.IOException;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author forhappy
 * Date: 2012-3-26, 10:54 PM.
 */


public class Table {
	
	
	public Table() {
		super();
		row = new RowSchema();
	}

	public Table(RowSchema row) {
		super();
		this.row = row;
	}

	public RowSchema generateRowSchema(Connection conn) {
		try {
			row.fillRowSchema(conn, tableName);
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return row;
	}
	
	public void fillRowSchema(Connection conn) {
		try {
			row.fillRowSchema(conn, tableName);
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	/**
	 * @return the tableCatalog
	 */
	public String getTableCatalog() {
		return tableCatalog;
	}

	/**
	 * @param tableCatalog the tableCatalog to set
	 */
	public void setTableCatalog(String tableCatalog) {
		this.tableCatalog = tableCatalog;
	}

	/**
	 * @return the tableSchema
	 */
	public String getTableSchema() {
		return tableSchema;
	}

	/**
	 * @param tableSchema the tableSchema to set
	 */
	public void setTableSchema(String tableSchema) {
		this.tableSchema = tableSchema;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the tableType
	 */
	public String getTableType() {
		return tableType;
	}

	/**
	 * @param tableType the tableType to set
	 */
	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	/**
	 * @return the remarks
	 */
	public String getRemarks() {
		return remarks;
	}

	/**
	 * @param remarks the remarks to set
	 */
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	
	/**
	 * @return the row
	 */
	public RowSchema getRowSchema() {
		return row;
	}

	/**
	 * @param row the row to set
	 */
	public void setRowSchema(RowSchema row) {
		this.row = row;
	}


	/**
	 * String object giving the table catalog, which may be null
	 */
	private String tableCatalog = null;
	
	/**
	 * String object giving the table schema, which may be null
	 */
	private String tableSchema = null;
	
	/**
	 * String object giving the table name
	 */
	private String tableName = null;
	
	/**
	 * String object giving the table type. Typical types are
	 * "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", 
	 * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 */
	private String tableType = null;
	
	/**
	 * String object containing an explanatory comment on the
	 * table, which may be null
	 */
	private String remarks = null;

	/**
	 * String object giving the types catalog; may be null
	 */
//	private String typeCatalog = null;
	
	/**
	 * String object giving the types schema; may be null
	 */
//	private String typeSchema = null;
	
	/**
	 * String object giving the type name; may be null
	 */
//	private String typeName = null;
	
	
	/**
	 * Row structure illustrates the table schema.
	 */
	
	private RowSchema row = null;
	private static final Logger logger = 
		LoggerFactory.getLogger(Table.class);
}