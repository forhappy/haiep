package cn.iie.haiep.rdbms.metadata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author forhappy
 * Date: 2012-3-26, 10:40 PM.
 */


/**
 * Row class indicates encapsulation of a record in rdbms, or rdbms schema.
 * 
 * @author forhappy
 * 
 */
public class RowSchema {
	/**
	 * default constructor
	 */
	public RowSchema() {
		super();
		listColumns = new ArrayList<Column>();
	}

	public RowSchema(List<Column> listColumns) {
		super();
		this.listColumns = listColumns;
	}

	public List<Column> getListColumns() {
		return listColumns;
	}

	/**
	 * @param listColumns
	 *            the listColumns to set
	 */
	public void setListColumns(List<Column> listColumns) {
		this.listColumns = listColumns;
	}

	public void fillRowSchema(Connection conn, String tableName)
			throws IOException {
		if (conn == null) {
			logger.info("Connection should not be null");
		}
		if (tableName == null) {
			logger.info("tableName should not be null");
		}

		/**
		 * connect and generate columns
		 */
		generateColumns(conn, tableName);
		generatePrimaryKeys(conn, tableName);
		while (nextColumn()) {
			addColumn();
		}
		
		/**
		 * find and set primary keys.
		 */
		
		setPrimaryKeys();
	}

	/**
	 * add a column to the RowSchema.
	 */
	private void addColumn(Column column) {
		listColumns.add(column);
	}

	/**
	 * add a default column schema to the RowSchema.
	 */
	private void addColumn() {
		Column column = new Column();
		try {
			column.setTableCatalog(rsColumns.getString("TABLE_CAT"));
			column.setTableSchema(rsColumns.getString("TABLE_SCHEM"));
			column.setTableName(rsColumns.getString("TABLE_NAME"));
			column.setColumnName(rsColumns.getString("COLUMN_NAME"));
			column.setDataType(rsColumns.getShort("DATA_TYPE"));
			column.setTypeName(rsColumns.getString("TYPE_NAME"));
			column.setColumnSize(rsColumns.getShort("COLUMN_SIZE"));
			column.setNullable(rsColumns.getInt("NULLABLE"));
			column.setRemarks(rsColumns.getString("REMARKS"));
			column.setColumnDefault(rsColumns.getString("COLUMN_DEF"));
			column.setOrdinalPosition(rsColumns.getInt("ORDINAL_POSITION"));
			column.setIsNullable(rsColumns.getString("IS_NULLABLE"));

			/**
			 * add a column
			 */
			addColumn(column);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * delete a column from the RowSchema.
	 */
	private void deleteColumn(Column column) {
		listColumns.remove(column);
	}

	/**
	 * add a list of columns to the RowSchema.
	 */
	private void addColumns(List<Column> columns) {
		listColumns.addAll(columns);
	}

	/**
	 * @return the listColumns
	 */

	private void generateColumns(Connection conn, String tableName) {
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			rsColumns = dbmd.getColumns(null, null, tableName, null);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private Boolean nextColumn() throws IOException {
		try {
			return rsColumns.next();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	private void setPrimaryKeys() {
		String columnName = "";
		try {
			while (nextPrimaryKey()) {
				try {
					columnName = rsPrimaryKeys.getString("COLUMN_NAME");
					if (columnName != null) {
						for (Column column : listColumns) {
							column = findColumn(columnName);
							column.setIsPrimaryKey(true);
						}
					}
				} catch (SQLException e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private void generatePrimaryKeys(Connection conn, String tableName) {
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			rsPrimaryKeys = dbmd.getPrimaryKeys(null, null, tableName);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private Boolean nextPrimaryKey() throws IOException {
		try {
			return rsPrimaryKeys.next();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		return false;
	}

	private Column findColumn(String columnName) {
		for (Column column : listColumns) {
			if (column.getColumnName().equalsIgnoreCase(columnName)) {
				return column;
			}
		}
		return null;
	}

	private ResultSet rsColumns = null;

	private ResultSet rsPrimaryKeys = null;

	/**
	 * columns in a row.
	 */
	private List<Column> listColumns = null;
	private static final Logger logger = LoggerFactory
			.getLogger(RowSchema.class);
}
