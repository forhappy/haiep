package cn.iie.haiep.hbase.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.iie.haiep.hbase.key.HKey;
import cn.iie.haiep.hbase.value.HColumn;
import cn.iie.haiep.hbase.value.HValue;
import cn.iie.haiep.rdbms.export.SQLExporter;


/**
 * Multithreaded Haiep for migrating database.
 * @author forhappy
 * Date: 2012-4-3, 9:35 PM.
 */

public class MigrateTableMT implements Runnable{

	/**
	 * TODO replace FAMILY with your desired string.
	 */
	public static final String FAMILY = "COMMONS";
	
	/**
	 * TODO replace REGION with your desired string.
	 */
	public static final String REGION = "HAIEP";
	
	/**
	 * write buffer size.
	 */
	private static final int WRITE_BUFFER_SIZE = 1024 * 1024 * 128;
	
	/**
	 *number of records in a list puts.  
	 */
	private static final int LIST_PUTS_COUNTER = 5000;
	
	private String tableName = null;
	
	List<Map<String, Object>> list = null;
	
	private HTablePool pool;

	private HTable table;

	private Configuration conf;
	
	public MigrateTableMT(String tableName, List<Map<String, Object>> list) {
		super();
		this.tableName = tableName;
		this.list = list;
	}

	public MigrateTableMT() {
	}
	
	/**
	 * Get HBaseConfigInfo manually
	 *
	 * @return
	 */
	public Configuration getConfig(){
		Configuration HBase_Config =  new Configuration();
		HBase_Config.set("hbase.zookeeper.quorum", "cloud006,cloud007,cloud008");
		HBase_Config.set("hbase.zookeeper.property.clientPort", "2181");
		Configuration config = new Configuration();
		config = HBaseConfiguration.create(HBase_Config);
		return config;
	}
	
	public void initialize() {
//		this.conf = HBaseConfiguration.create();
		this.conf = getConfig();
//		conf.set("hbase.zookeeper.quorum", "cloud006,cloud007,cloud008");
		pool = new HTablePool(conf, 1024);
	}
	

	/**
	 * migrate table by list puts.
	 * @param tableName Table name in Hbase which table will be migrated to. 
	 * @param list Data in RDBMS. 
	 */
	public void migrateDataByBatch(String tableName, List<Map<String, Object>> list) {
		String tableNamePacked = REGION + "." + tableName;
		/**
		 * table to migrate data.
		 */
		HTable table = (HTable) pool.getTable(tableNamePacked);

		/**w
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

		for (int i = 0; i < list.size(); i++) {

			Put put = new Put((new Integer(i)).toString().getBytes());

			Map<String, Object> map = list.get(i);
			counter++;
			/**
			 * add one row to be put.
			 */
			for (Map.Entry<String, Object> m : map.entrySet()) {
				put.add(FAMILY.getBytes(), m.getKey().getBytes(), m.getValue()
						.toString().getBytes());

			}
			/**
			 * add `put` to list puts.
			 */
			puts.add(put);

			if ((counter % LIST_PUTS_COUNTER == 0) || (i == list.size() - 1)) {
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
	
	/**
	 * migrate table put by put.
	 * @param tableName Table name in Hbase which table will be migrated to. 
	 * @param list Data in RDBMS. 
	 */
	public void migrateData(String tableName, List<Map<String, Object>> list) {
		String tableNamePacked = REGION + "." + tableName;
		/**
		 * table to migrate data.
		 */
		HTable table = (HTable) pool.getTable(tableNamePacked);
		for (int i = 0; i < list.size(); i++) {
			Put put = new Put((new Integer(i)).toString().getBytes());
			Map<String, Object> map = list.get(i);
			for (Map.Entry<String, Object> m : map.entrySet()) {
				put.add(FAMILY.getBytes(), m.getKey().getBytes(), m.getValue()
						.toString().getBytes());
			}
			try {
				table.put(put);
			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
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
	 * @return the list
	 */
	public List<Map<String, Object>> getList() {
		return list;
	}

	/**
	 * @param list the list to set
	 */
	public void setList(List<Map<String, Object>> list) {
		this.list = list;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static final Logger logger = LoggerFactory.getLogger(MigrateTableMT.class);
	
	@Override
	public void run() {
		initialize();
		migrateDataByBatch(tableName, list);
	}
}
