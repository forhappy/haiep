package cn.iie.haiep.hbase.admin;

/**
 * Meta table description.
 * @author forhappy
 * Date: 2012-4-3, 9:35 PM.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.iie.haiep.hbase.store.HBaseTableInfo;
import cn.iie.haiep.rdbms.metadata.Column;
import cn.iie.haiep.rdbms.metadata.Database;
import cn.iie.haiep.rdbms.metadata.RowSchema;
import cn.iie.haiep.rdbms.metadata.Table;

public class MetaTable {
	
	private HBaseAdmin admin;
	
	private HTable table;

	private Configuration conf;
	
	private boolean autoCreateSchema = true;
	
	public static final String tableName = "METATABLE";
	
	/**
	 * TODO replace FAMILY with your desired string.
	 */
	public static final String FAMILY = "HAIEP";
	
	/**
	 * TODO replace REGION with your desired string.
	 */
	public static final String REGION = "HAIEP";
	
	/**
	 * @return the family
	 */
	public static String getFamily() {
		return FAMILY;
	}

	/**
	 * @return the region
	 */
	public static String getRegion() {
		return REGION;
	}

	public void importMetadataFromRDBMS(
			String url,
			String user,
			String password,
			String catalog) {
		HTablePool pool = new HTablePool(conf, 1024);
		HTable table = (HTable) pool.getTable(tableName);
		
		Database db = new Database(url, user, password, catalog);
		db.fillTableLists();

//		String rowkey = "dummy";
//		Put put = new Put(rowkey.getBytes());
//		put.add(family, qualifier, value);

		while(db.hasNextTable()) {
			Table tbl = db.next();
			
			/**
			 * beginOfRowkey = REGION.TABLE_NAME
			 */
			String beginOfRowkey = REGION + "." + tbl.getTableName(); 
			RowSchema rowSchema = tbl.getRowSchema();
			while(rowSchema.hasNextColumn()) {
				Column column = rowSchema.next();
				
				/**
				 * rowkey = REGION.TABLENAME.COLUMN_NAME
				 */
				String rowkey = beginOfRowkey + "." + column.getColumnName();
				Put put = new Put(rowkey.getBytes());
				byte[] familyBytes = getFamily().getBytes();
				put.add(familyBytes, "IsNil".getBytes(), column.getIsNullable().getBytes());
				put.add(familyBytes, "Type".getBytes(), column.getTypeName().getBytes());
				Integer len = new Integer(column.getColumnSize());
				put.add(familyBytes, "Len".getBytes(), len.toString().getBytes());
				put.add(familyBytes, "IsKey".getBytes(), column.getIsPrimaryKey().toString().getBytes());
				try {
					table.put(put);
				} catch (IOException e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * initialize conf and admin.
	 */
	public void initialize() {
		this.conf = HBaseConfiguration.create();
		try {
			this.admin = new HBaseAdmin(conf);
			/**
			 * initializing tableInfo.
			 */
			
			/**
			 * set table name. 
			 */
			addTableName(tableName);
			
			/**
			 * add table.
			 */
			addTable(tableName);
			
			/**
			 * add family
			 */
			addColumnFamily(tableName, null);
			
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		if (autoCreateSchema) {
			try {
				createSchema(tableName);
			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * create schema.
	 * @throws IOException
	 */
	public void createSchema(String tableName) throws IOException {
		if (admin.tableExists(tableInfo.getTableName(tableName))) {
			return;
		}
		HTableDescriptor tableDesc = tableInfo.getTable(tableName);

		admin.createTable(tableDesc);
	}
	
	/**
	 * Add table name.
	 * @param tableName table name to be set.
	 */
	public void addTableName(String tableName) {
		if (tableInfo == null) {
			logger.warn("MetaTable object has to be constructed at first.");
			return;
		}
		tableInfo.addTableName(tableName);
	}
	
	/**
	 * add table.
	 * @param tableName
	 */
	public void addTable(String tableName) {
		tableInfo.addTable(tableName);
	}
	
	/**
	 * add family name.
	 * @param familyName family name.
	 */
	public void addColumnFamily(String tableName, String familyName) {
		if (familyName != null) {
			tableInfo.addColumnFamily(tableInfo.getTableName(tableName), 
					familyName, null, null, null, null, null, null, null);
		} else {
			tableInfo.addColumnFamily(tableInfo.getTableName(tableName), FAMILY,
					null, null, null, null, null, null, null);
		}
	}
	
	/**
	 * Get table name.
	 * @return table name.
	 */
	public String getTableName(String tableName) {
		if (tableInfo == null) {
			logger.warn("MetaTable object has to be constructed at first.");
			return null;
		}
		return tableInfo.getTableName(tableName);
	}
	
	/**
	 * meta table info.
	 */
	HBaseTableInfo tableInfo = null;
	
	/**
	 * Default constructor. 
	 */
	public MetaTable() {
		tableInfo = new HBaseTableInfo();
		tableInfo.addTableName(tableName);
	}

	public MetaTable(HBaseTableInfo tableInfo) {
		super();
		this.tableInfo = tableInfo;
	}

	/**
	 * @return the tableInfo
	 */
	public HBaseTableInfo getTableInfo() {
		return tableInfo;
	}

	/**
	 * @param tableInfo the tableInfo to set
	 */
	public void setTableInfo(HBaseTableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}
	
	public void deleteSchema(String tableName) throws IOException {
		if (!admin.tableExists(tableInfo.getTableName(tableName))) {
			if (table != null) {
				table.getWriteBuffer().clear();
			}
			return;
		}
		admin.disableTable(tableInfo.getTableName(tableName));
		admin.deleteTable(tableInfo.getTableName(tableName));
	}

	public boolean schemaExists(String tableName) throws IOException {
		return admin.tableExists(tableInfo.getTableName(tableName));
	}

	public void flush() throws IOException {
		table.flushCommits();
	}

	public void close() throws IOException {
		flush();
		if (table != null)
			table.close();
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	/**
	 * logger.
	 */
	public static final Logger logger = LoggerFactory.getLogger(MetaTable.class);
}
