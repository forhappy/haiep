package cn.iie.haiep.hbase.admin;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

import cn.iie.haiep.hbase.key.HKey;
import cn.iie.haiep.hbase.store.HBaseTableInfo;
import cn.iie.haiep.hbase.value.HColumn;
import cn.iie.haiep.hbase.value.HValue;
import cn.iie.haiep.rdbms.export.SQLExporter;
import cn.iie.haiep.rdbms.export.SQLMetaExporter;
import cn.iie.haiep.rdbms.metadata.Database;
import cn.iie.haiep.rdbms.metadata.Table;

public class HaiepExporter {

	/**
	 * TODO replace FAMILY with your desired string.
	 */
	public static final String FAMILY = "COMMONS";
	
	/**
	 * TODO replace REGION with your desired string.
	 */
	public static final String REGION = "HAIEP";
	
	private HBaseAdmin admin;
	
	private HTablePool pool;

	private HTable table;

	private Configuration conf;

	private boolean autoCreateSchema = true;

	private HBaseTableInfo tableInfo;
	
	private String username = null;
	
	private String password = null;
	
	private String url = null;
	
	private String catalog = null;
	
	private SQLMetaExporter sqlMetaExporter = null;
	
	/**
	 * write buffer size.
	 */
	private static final int WRITE_BUFFER_SIZE = 1024 * 1024 * 256;
	
	/**
	 *number of records in a list puts.  
	 */
	private static final int LIST_PUTS_COUNTER = 500;
	
	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}
	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
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
	
	
	public HaiepExporter(String username, String password, String url,
			String catalog) {
		super();
		this.username = username;
		this.password = password;
		this.url = url;
		this.catalog = catalog;
		sqlMetaExporter = new SQLMetaExporter(url, username, password, catalog);
	}
	
	public HaiepExporter() {
	}
	
	/**
	 * Get HBaseConfigInfo manually
	 *
	 * @return
	 */
	public Configuration getConfig(){
		Configuration hbaseConfig =  new Configuration();
//		hbaseConfig.set("hbase.zookeeper.quorum", "cloud006,cloud007,cloud008");
		hbaseConfig.set("hbase.zookeeper.quorum", "master,slave");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		Configuration config = new Configuration();
		config = HBaseConfiguration.create(hbaseConfig);
		return config;
	}
	
	public void initialize() {
//		this.conf = getConfig();
		this.conf = HBaseConfiguration.create();
		try {
			this.admin = new HBaseAdmin(conf);
			tableInfo = new HBaseTableInfo();
			initTableInfo();
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		if (autoCreateSchema) {
			try {
				createSchemas();
			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
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
	
	public void createSchemas() throws IOException {
		if (tableInfo != null) {
			while (tableInfo.hasNextTableName()) {
				String tableName = tableInfo.nextTableName();
				createSchema(tableName);
			}
		}
	}

	
	public void createSchema(String tableName) throws IOException {
		if (admin.tableExists(tableInfo.getTableName(tableName))) {
			return;
		}
		HTableDescriptor tableDesc = tableInfo.getTable(tableName);

		admin.createTable(tableDesc);
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
	
	public void put(String tableName, HKey key, HValue value) {
		pool = new HTablePool(conf, 1024);
		HTable table = (HTable) pool.getTable(tableName);
		
		Put put = new Put(key.getRow());
		while (value.hasNext()) {
			HColumn column = value.next();
			byte[] familyBytes = column.getFamily();
			byte[] qualifierBytes = column.getQualifier();
			byte[] valueBytes = column.getValue();
			put.add(familyBytes, qualifierBytes, valueBytes);
		}
		try {
			table.put(put);
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void migrateData() {
		pool = new HTablePool(conf, 1024);
		SQLExporter sqlExporter = 
			new SQLExporter(url, username, password, catalog);
		while(sqlExporter.hasNextDataTable()) {
			Entry entry = sqlExporter.next();
			String tableName = REGION + "." + (String) entry.getKey();
			List<Map<String, Object>> list = (List<Map<String, Object>>) entry.getValue();
			/**
			 * table to migrate data.
			 */
			HTable table = (HTable) pool.getTable(tableName);
			int size = list.size();
			for (int i = 0; i < size; i++) {
				
				Put put = new Put((new Integer(i)).toString().getBytes());
				
				Map<String, Object> map = list.get(i);
				for (Map.Entry<String, Object> m : map.entrySet()) {
					
					put.add(FAMILY.getBytes(), m.getKey().getBytes(), m.getValue().toString().getBytes());	
//					System.out.println("Key: " + m.getKey());
//					System.out.println("Value: " + m.getValue());		
				}
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
	 * Migrate table by list puts.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void migrateDataByBatch() {
		pool = new HTablePool(conf, 1024);
		SQLExporter sqlExporter = 
			new SQLExporter(url, username, password, catalog);
		while(sqlExporter.hasNextDataTable()) {
			Entry entry = sqlExporter.next();
			String tableName = REGION + "." + (String) entry.getKey();
			List<Map<String, Object>> list = (List<Map<String, Object>>) entry.getValue();
			/**
			 * table to migrate data.
			 */
			HTable table = (HTable) pool.getTable(tableName);
			
			/**
			 * set write buffer size.
			 */
			try {
				table.setWriteBufferSize(WRITE_BUFFER_SIZE);
				table.setAutoFlush(false);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			int counter = 0;
			List<Put> puts = new ArrayList<Put>();
			int size = list.size();
			for (int i = 0; i < size; i++) {
				
				Put put = new Put((new Integer(i)).toString().getBytes());
				
				Map<String, Object> map = list.get(i);
				counter ++;
				/**
				 * add one row to be put.
				 */
				for (Map.Entry<String, Object> m : map.entrySet()) {	
					put.add(FAMILY.getBytes(), m.getKey().getBytes(), 
							m.getValue().toString().getBytes());	
	
				}
				/**
				 * add `put` to list puts. 
				 */
				puts.add(put);
				
				if ((counter % LIST_PUTS_COUNTER == 0) || (i == size - 1)) {
					try {
						table.put(puts);
						table.flushCommits();
						puts.clear();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else continue;
			}
		}
	}
	
	/**
	 * Migrate table by list puts.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void migrateDataByBatch(String tableName, List<Map<String, Object>> list) {
		pool = new HTablePool(conf, 1024);
		String tableNameFormated = REGION + "." + tableName;
		/**
		 * table to migrate data.
		 */
		HTable table = (HTable) pool.getTable(tableNameFormated);
		
		/**
		 * set write buffer size.
		 */
		try {
			table.setWriteBufferSize(WRITE_BUFFER_SIZE);
			table.setAutoFlush(false);
		} catch (IOException e) {
			e.printStackTrace();
		}

		int counter = 0;
		List<Put> puts = new ArrayList<Put>();
		int size = list.size();
		for (int i = 0; i < size; i++) {

			Put put = new Put((new Integer(i)).toString().getBytes());

			Map<String, Object> map = list.get(i);
			counter++;
			/**
			 * add one row to be put.
			 */
			for (Map.Entry<String, Object> m : map.entrySet()) {
				Object qualifier = m.getKey();
				Object value = m.getValue();
				if (qualifier != null && value !=null)
					put.add(FAMILY.getBytes(), qualifier.toString().getBytes(), value.toString().getBytes());
				else if (qualifier != null && value ==null)
					put.add(FAMILY.getBytes(), qualifier.toString().getBytes(), null);
			}
			/**
			 * add `put` to list puts.
			 */
			puts.add(put);

			if ((counter % LIST_PUTS_COUNTER == 0) || (i == size - 1)) {
				try {
					table.put(puts);
					table.flushCommits();
					puts.clear();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else
				continue;
		}
	}

	public void export() {
		while(sqlMetaExporter.hasNextTable()) {
			String tableName = sqlMetaExporter.nextTable();
			logger.info("Importing table " + tableName + "...");
			List<Map<String, Object>> list = sqlMetaExporter.buildTableInMemory(tableName);
			migrateDataByBatch(tableName, list);
			logger.info("Table " + tableName + " imported!");
		}
	}
	
	public void export(int rowNum) {
		while(sqlMetaExporter.hasNextTable()) {
			String tableName = sqlMetaExporter.nextTable();
			
			PreparedStatement pstmt = sqlMetaExporter.getPreStmtsMap().get(tableName);
			if (pstmt == null) {
				logger.info("Skipping importing table " + tableName + "...");
				continue;
			}
			try {
				ResultSet rs = pstmt.executeQuery();
				logger.info("Beginning importing table " + tableName + "...");
				exportTableFromResultSet(tableName, rs, rowNum);
			} catch (SQLException e) {
				logger.info(e.getMessage());
				e.printStackTrace();
			}
			logger.info("Table " + tableName + " imported!");
		}
	}
	
	/**
	 * export data from resultset directly.
	 * @param tableName
	 * @return a list structure that contains all data records in the RDBMS.
	 */
	private void exportTableFromResultSet(String tableName, ResultSet rs, int rowNum) {
		Map<String, String> columnMetadataMap = sqlMetaExporter.getMetadataMap().get(tableName);
		List<Map<String, Object>> listData = new ArrayList<Map<String, Object>>();
		int counter = 0;
		try {
			while (rs.next()) {
				counter++;
				Map<String, Object> map = new HashMap<String, Object>();

				for (Map.Entry<String, String> columnMap : columnMetadataMap
						.entrySet()) {
					String columnLabel = columnMap.getKey();
					Object object = rs.getObject(columnLabel);
					//convert orcale timestamp to java date.
					if (object instanceof oracle.sql.TIMESTAMP) {
						oracle.sql.TIMESTAMP timeStamp = (oracle.sql.TIMESTAMP) object;
						Timestamp tt = timeStamp.timestampValue();
						Date date = new Date(tt.getTime());
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						String timestamp = sdf.format(date);
						map.put(columnLabel, timestamp);
					} else 
						map.put(columnLabel, object);
				}
				listData.add(map);
				if (counter % rowNum == 0) {
					migrateDataByBatch(tableName, listData);
					listData.clear();
				} else continue;
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			migrateDataByBatch(tableName, listData);
			listData.clear();
		}
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

	public static final Logger logger = LoggerFactory.getLogger(HaiepAdmin.class);
	
	/**
	 * Initializing tableInfo
	 */
	@SuppressWarnings("rawtypes")
	private void initTableInfo() {
//		Database db = new Database(url, username, password, catalog);
//		db.fillTableLists();
		Database db = sqlMetaExporter.getDatabase();
		Iterator iter = db.iterator();
		while (iter.hasNext()) {
			Table table = (Table) iter.next();
			/**
			 * Get table name.
			 */
			String tableName = REGION + "." + table.getTableName();
			addTableName(tableName);
			addTable(tableName);
			addColumnFamily(tableName, null);
		}
//		while (db.hasNextTable()) {
//			Table tbl = db.next();
//			
//			/**
//			 * Get table name.
//			 */
//			String tableName = REGION + "." + tbl.getTableName();
//			addTableName(tableName);
//			addTable(tableName);
//			addColumnFamily(tableName, null);
//		}
	}
}
