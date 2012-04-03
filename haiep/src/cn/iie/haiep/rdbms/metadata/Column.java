package cn.iie.haiep.rdbms.metadata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author forhappy
 * Date: 2012-3-26, 10:34 PM.
 */

public class Column {
	public Column() {
		super();
	}

	public Column(String columnName, short dataType, String typeName,
			int columnSize, int nullable, String columnDefault,
			int ordinalPosition) {
		super();
		this.columnName = columnName;
		this.dataType = dataType;
		this.typeName = typeName;
		this.columnSize = columnSize;
		this.nullable = nullable;
		this.columnDefault = columnDefault;
		this.ordinalPosition = ordinalPosition;
	}
	

	/**
	 * is primary key, default is false.
	 */
	private Boolean isPrimaryKey = false;
	
	/**
	 * is foreign key, default is false.
	 */
	private Boolean isForeignKey = false;
	
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
	 * String object giving the column name
	 */
	private String columnName = null;
	
	/**
	 * short indicating the JDBC (SQL) data type from java.sql.Types
	 */
	private short dataType = 0;
	
	/**
	 * String object giving the local type name used by the data source
	 */
	private String typeName = null;
	
	/**
	 * int indicating the column size. For char or date types, this is
	 * the maximum number of characters; for numeric or decimal
	 * types, this is the precision.
	 */
	private int columnSize = 0;
	
	/**
	 * int indicating whether a column can be NULL
	 * The possible values are:
	 *   columnNoNulls - NULL values might not be allowed
	 *   columnNullable - NULL values are definitely allowed
	 *   columnNullableUnknown - whether NULL values are allowed is unknown
	 */
	private int nullable = 0;
	
	/**
	 * String object containing an explanatory comment on the column; may be null
	 */
	private String remarks = null;
	
	/**
	 * String object containing the default value for the column; may be null
	 */
	
	private String columnDefault = null;
	
	/**
	 * int indicating the index of the column in a table; the first column is 1, 
	 * the second column is 2, and so on
	 */
	private int ordinalPosition = 0; 
	
	/**
	 * String object; either NO indicating that the column definitely
	 * does not allow NULL values, YES indicating that the column
	 * might allow NULL values, or an empty string ("") indicating
	 * that nullability is unknown
	 */
	private String isNullable = null;

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
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * @return the dataType
	 */
	public short getDataType() {
		return dataType;
	}

	/**
	 * @param dataType the dataType to set
	 */
	public void setDataType(short dataType) {
		this.dataType = dataType;
	}

	/**
	 * @return the typeName
	 */
	public String getTypeName() {
		return typeName;
	}

	/**
	 * @param typeName the typeName to set
	 */
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	/**
	 * @return the columnSize
	 */
	public int getColumnSize() {
		return columnSize;
	}

	/**
	 * @param columnSize the columnSize to set
	 */
	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}

	/**
	 * @return the nullable
	 */
	public int getNullable() {
		return nullable;
	}

	/**
	 * @param nullable the nullable to set
	 */
	public void setNullable(int nullable) {
		this.nullable = nullable;
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
	 * @return the columnDefault
	 */
	public String getColumnDefault() {
		return columnDefault;
	}

	/**
	 * @param columnDefault the columnDefault to set
	 */
	public void setColumnDefault(String columnDefault) {
		this.columnDefault = columnDefault;
	}

	/**
	 * @return the ordinalPosition
	 */
	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	/**
	 * @param ordinalPosition the ordinalPosition to set
	 */
	public void setOrdinalPosition(int ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}

	/**
	 * @return the isNullable
	 */
	public String getIsNullable() {
		return isNullable;
	}

	/**
	 * @param isNullable the isNullable to set
	 */
	public void setIsNullable(String isNullable) {
		this.isNullable = isNullable;
	}

	/**
	 * @return the isPrimaryKey
	 */
	public Boolean getIsPrimaryKey() {
		return isPrimaryKey;
	}

	/**
	 * @param isPrimaryKey the isPrimaryKey to set
	 */
	public void setIsPrimaryKey(Boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	/**
	 * @return the isForeignKey
	 */
	public Boolean getIsForeignKey() {
		return isForeignKey;
	}

	/**
	 * @param isForeignKey the isForeignKey to set
	 */
	public void setIsForeignKey(Boolean isForeignKey) {
		this.isForeignKey = isForeignKey;
	} 
	
	private static final Logger logger = 
		LoggerFactory.getLogger(Column.class);
}