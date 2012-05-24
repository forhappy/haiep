package cn.iie.haiep.hbase.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class QueryTerminalTraderInfo extends HaiepQuery {
	/**
	 * Get HBaseConfigInfo manually
	 *
	 * @return
	 */
	static public Configuration getConfig(){
		Configuration hbaseConfig =  new Configuration();
//		hbaseConfig.set("hbase.zookeeper.quorum", "cloud006,cloud007,cloud008");
//		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		Configuration config = new Configuration();
		config = HBaseConfiguration.create(hbaseConfig);
		return config;
	}
	
	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
	}
	
/**
	 * 网吧日核查数量。
	 * @param date 日期格式20120101
	 * @param terminalTrader 网吧代码
	 * @return 网吧日核查数量。
	 */
	static public int queryNumOfCheckLogs(String date, String terminalTrader) {
		HTablePool pool = new HTablePool(getConfig(), 1024);
		HTable table = (HTable) pool.getTable(Constants.TRADER_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.TRADER_CHECK_NUMBER[0], Constants.TRADER_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(terminalTrader + date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = new Integer(new String(keyValue.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
}
