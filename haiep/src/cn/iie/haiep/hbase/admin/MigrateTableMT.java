package cn.iie.haiep.hbase.admin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
	
	public void initialize() {
		this.conf = HBaseConfiguration.create();
		pool = new HTablePool(conf, 1024);
	}
	
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
		migrateData(tableName, list);
	}
}
