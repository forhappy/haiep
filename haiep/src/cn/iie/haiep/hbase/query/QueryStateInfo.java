package cn.iie.haiep.hbase.query;

import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

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

import com.google.gson.Gson;

public class QueryStateInfo extends HaiepQuery {
	/**
	 * Get HBaseConfigInfo manually
	 *
	 * @return
	 */
	static public Configuration getConfig(){
		Configuration hbaseConfig =  new Configuration();
		hbaseConfig.set("hbase.zookeeper.quorum", "192.168.1.230,192.168.1.234");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		Configuration config = new Configuration();
		config = HBaseConfiguration.create(hbaseConfig);
		return config;
	}
	
	public static Configuration configuration;
	static {
//		configuration = HBaseConfiguration.create();
		configuration = getConfig();
	}
	
	/**
	 * 查询全国日网吧数量。（不包含当天）
	 * @param date 日期格式20120101
	 * @return 全国日网吧数量。（不包含当天）
	 */
	static public int queryNumOfNetbarsByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_TRADER[0], Constants.STATE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国日网吧新增数量。
	 * @param date 日期格式20120101
	 * @return 全国日网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_TRADER[0], Constants.STATE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国日代理商数量。（不包含当天）
	 * @param date 日期格式20120101
	 * @return 全国日代理商数量。（不包含当天）
	 */
	static public int queryNumOfAgentsByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_AGENT[0], Constants.STATE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国日代理商新曾数量。
	 * @param date 日期格式20120101
	 * @return 全国日代理商新曾数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_AGENT[0], Constants.STATE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	/**
	 * 查询全国活跃网吧数量。(不包含当天)
	 * @param date 日期格式20120101
	 * @return 全国活跃网吧数量。(不包含当天)
	 */
	static public int queryNumOfActiveNetbarsByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_ACTIVE_TRADER[0], Constants.STATE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国非活跃网吧数量。
	 * @param date 日期格式20120101
	 * @return 全国非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_NON_ACTIVE_TRADER[0], Constants.STATE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国日核查数量。
	 * @param date 日期格式20120101
	 * @return 全国日核查数量。
	 */
	static public int queryNumOfCheckLogsByDay(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_CHECK_NUMBER[0], Constants.STATE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国月网吧数量。
	 * @param date 日期格式201201
	 * @return 全国月网吧数量。
	 */
	static public int queryNumOfNetbarsByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_TRADER[0], Constants.STATE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国月网吧新增数量。
	 * @param date 日期格式201201
	 * @return 全国月网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_TRADER[0], Constants.STATE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国月代理商数量。
	 * @param date 日期格式201201
	 * @return 全国月代理商数量。
	 */
	static public int queryNumOfAgentsByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_AGENT[0], Constants.STATE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国月代理商新增数量。
	 * @param date 日期格式201201
	 * @return 全国月代理商新增数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_AGENT[0], Constants.STATE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	/**
	 * 查询全国月活跃网吧数量。
	 * @param date 日期格式201201
	 * @return 全国月活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_ACTIVE_TRADER[0], Constants.STATE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国月非活跃网吧数量。
	 * @param date 日期格式201201
	 * @return 全国月非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_NON_ACTIVE_TRADER[0], Constants.STATE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度核查数量。
	 * @param date 日期格式201201
	 * @return 全国季度核查数量。
	 */
	static public int queryNumOfCheckLogsByMonth(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_CHECK_NUMBER[0], Constants.STATE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度网吧数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度网吧数量。
	 */
	static public int queryNumOfNetbarsByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_TRADER[0], Constants.STATE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度网吧新增数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_TRADER[0], Constants.STATE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度代理商数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度代理商数量。
	 */
	static public int queryNumOfAgentsByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_AGENT[0], Constants.STATE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度代理商新增数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度代理商新增数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_AGENT[0], Constants.STATE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	/**
	 * 查询全国季度活跃网吧数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_ACTIVE_TRADER[0], Constants.STATE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度非活跃网吧数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_NON_ACTIVE_TRADER[0], Constants.STATE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国季度核查数量。
	 * @param date 日期格式2012X(x表示季度，如1表示第一季度，
	 *                  2表示第二季度，3表示第三季度，4表示第四季度)
	 * @return 全国季度核查数量。
	 */
	static public int queryNumOfCheckLogsByQuarter(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_CHECK_NUMBER[0], Constants.STATE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国年网吧数量。
	 * @param date 日期格式2012
	 * @return 全国年网吧数量。
	 */
	static public int queryNumOfNetbarsByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_TRADER[0], Constants.STATE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国年网吧新增数量。
	 * @param date 日期格式2012
	 * @return 全国年网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_TRADER[0], Constants.STATE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国年代理商数量。
	 * @param date 日期格式2012
	 * @return 全国年代理商数量。
	 */
	static public int queryNumOfAgentsByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_AGENT[0], Constants.STATE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国年代理商新增数量。
	 * @param date 日期格式2012
	 * @return 全国年代理商新增数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_INCRE_AGENT[0], Constants.STATE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	/**
	 * 查询全国年活跃网吧数量。
	 * @param date 日期格式2012
	 * @return 全国年活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_ACTIVE_TRADER[0], Constants.STATE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国年非活跃网吧数量。
	 * @param date 日期格式2012
	 * @return 全国年非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_DAILY_NON_ACTIVE_TRADER[0], Constants.STATE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国年核查数量。
	 * @param date 日期格式2012
	 * @return 全国年核查数量。
	 */
	static public int queryNumOfCheckLogsByYear(String date) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_STATE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.STATE_CHECK_NUMBER[0], Constants.STATE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(date)));
		
		try {
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue keyValue : r.raw()) {
					result = Bytes.toInt(keyValue.getValue());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 查询全国日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 json 格式的全国统计信息 。
	 * {
	 *   "netbars" : 4300,
	 *   "netbars-added" : 200,
	 *   "agents" : 80,
	 *   "agents-added" : 5,
	 *   "actives" : 42300,
	 *   "non-actives" : 700,
	 *   "check-logs": 90000
	 * }
	 */
	static public String queryStateInfoByDay(String date) {
		String result = "";
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByDay(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByDay(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date)));
		
		Gson gson = new Gson();
		result = gson.toJson(statsMap);
		return result;
	} 
	
	/**
	 * 查询全国日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的全国统计信息 。
	 */
	static private Map<String, Integer> queryStateMapInfoByDay(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByDay(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByDay(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date)));

		return statsMap;
	} 
	
	/**
	 * 查询全国日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的全国统计信息 。
	 */
	static public Map<String, Integer> queryStateMapInfoByDayForProvinceStats(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByDay(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByDay(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date)));

		return statsMap;
	} 
	
	/**
	 * 查询全国日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的全国统计信息 。
	 */
	static public Map<String, Integer> queryStateMapInfoByDayForAgentStats(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date)));
//		statsMap.put("agents", new Integer(queryNumOfAgentsByDay(date)));
//		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByDay(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date)));

		return statsMap;
	}
	
	/**
	 * 查询全国月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 json 格式的全国统计信息 。
	 * {
	 *   "netbars" : 4300,
	 *   "netbars-added" : 200,
	 *   "agents" : 80,
	 *   "agents-added" : 5,
	 *   "actives" : 42300,
	 *   "non-actives" : 700,
	 *   "check-logs": 90000
	 * }
	 */
	static public String queryStateSingleInfoByMonth(String date) {
		String result = "";
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByMonth(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByMonth(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date)));
		
		Gson gson = new Gson();
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询全国月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static private Map<String, Integer> queryStateMapInfoByMonth(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByMonth(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByMonth(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date)));
		return statsMap;
	}
	
	/**
	 * 查询全国月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static public Map<String, Integer> queryStateMapInfoByMonthForProvinceStats(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByMonth(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByMonth(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date)));
		return statsMap;
	}
	
	/**
	 * 查询全国月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static public Map<String, Integer> queryStateMapInfoByMonthForAgentStats(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date)));
//		statsMap.put("agents", new Integer(queryNumOfAgentsByMonth(date)));
//		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByMonth(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date)));
		return statsMap;
	}
	
	
	/**
	 * 查询全国季度统计信息。
	 * @param date 日期格式2012X(X表示季度，1表示第1季度，2表示第2季度等)
	 * @return 返回 json 格式的全国统计信息 。
	 * {
	 *   "netbars" : 4300,
	 *   "netbars-added" : 200,
	 *   "agents" : 80,
	 *   "agents-added" : 5,
	 *   "actives" : 42300,
	 *   "non-actives" : 700,
	 *   "check-logs": 90000
	 * }
	 */
	static public String queryStateSingleInfoByQuarter(String date) {
		String result = "";
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByQuarter(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByQuarter(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByQuarter(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByQuarter(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByQuarter(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByQuarter(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByQuarter(date)));
		
		Gson gson = new Gson();
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询全国季度统计信息。
	 * @param date 日期格式2012X(X表示季度，1表示第1季度，2表示第2季度等)
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static private Map<String, Integer> queryStateMapInfoByQuarter(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByQuarter(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByQuarter(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByQuarter(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByQuarter(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByQuarter(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByQuarter(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByQuarter(date)));

		return statsMap;
	}
	
	/**
	 * 查询全国年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 json 格式的全国统计信息 。
	 * {
	 *   "netbars" : 4300,
	 *   "netbars-added" : 200,
	 *   "agents" : 80,
	 *   "agents-added" : 5,
	 *   "actives" : 42300,
	 *   "non-actives" : 700,
	 *   "check-logs": 90000
	 * }
	 */
	static public String queryStateSingleInfoByYear(String date) {
		String result = "";
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByYear(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByYear(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date)));
		
		Gson gson = new Gson();
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询全国年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static private Map<String, Integer> queryStateMapInfoByYear(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByYear(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByYear(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date)));

		return statsMap;
	}
	
	/**
	 * 查询全国年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static public Map<String, Integer> queryStateMapInfoByYearForProvinceStats(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByYear(date)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByYear(date)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date)));

		return statsMap;
	}
	
	/**
	 * 查询全国年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static public Map<String, Integer> queryStateMapInfoByYearForAgentStats(String date) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date)));

		return statsMap;
	}
	
	/**
	 * 查询全国月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 json 格式的全国统计信息 。
	 * 
	 * {
	 *   "2012-04-01":
	 *     {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "2012-04-02":
	 *     {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     ...
	 *     "2012-04-30":
	 *     {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "total": {
     *       "netbars": 43000,
     *       "netbars-added": 2000,
     *       "agents": 800,
     *       "agents-added": 50,
     *       "actives": 423000,
     *       "non-actives": 7000,
     *       "check-logs": 900000
     *     }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryStateInfoByMonth(String date) {
		String result = "";
		
		String year = date.substring(0, 4);
		String month = date.substring(4, 6);
		Calendar cal=Calendar.getInstance();
		cal.clear();
		cal.set(Calendar.YEAR, new Integer(year));
		cal.set(Calendar.MONTH, new Integer(month) - 1);
		int days = cal.getActualMaximum(Calendar.DAY_OF_MONTH);//本月份的天数;
		
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		for(int i = 1; i <= days; i++) {
			if (i < 10) {
				String day = "0" + i;
				String statsKey = year + "-" + month + "-" + day;
				String queryDate = year + month + day;
				Map<String, Integer> statsValue = queryStateMapInfoByDay(queryDate);
				statsMap.put(statsKey, statsValue);
			} else {
				String day = (new Integer(i)).toString();
				String statsKey = year + "-" + month + "-" + day;
				String queryDate = year + month + day;
				Map<String, Integer> statsValue = queryStateMapInfoByDay(queryDate);
				statsMap.put(statsKey, statsValue);
			}
		}
		Gson gson = new Gson();
		//获取月份的综合统计信息。
		Map<String, Integer> totalInfo = queryStateMapInfoByMonth(date);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询全国季度统计信息。
	 * @param date 日期格式2012
	 * @return 返回 json 格式的全国季度统计信息 。
	 * 
	 * {
	 *   "第1季度":
	 *     {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "第2季度":
	 *     {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "第3季度":
	 *     {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "第4季度":
	 *     {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryStateInfoByQuater(String date) {
		String result = "";

		Map<String, Map>  quarterStatsMap = new LinkedHashMap<String, Map>();
		for(int i = 1; i <= 4; i++) {
				String statsKey = "第" + i + "季度";
				String queryDate = date + i;
				Map<String, Integer> statsValue = queryStateMapInfoByQuarter(queryDate);
				quarterStatsMap.put(statsKey, statsValue);
		}
		Gson gson = new Gson();
		result = gson.toJson(quarterStatsMap);

		return result;
	}
	
	/**
	 * 查询全国年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 json 格式的全国统计信息 。
	 * 
	 * {
	 *   "2012-01":
	 *     {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "2012-02":
	 *     {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     ...
	 *     "2012-12":
	 *     {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "total": {
     *       "netbars": 43000,
     *       "netbars-added": 2000,
     *       "agents": 800,
     *       "agents-added": 50,
     *       "actives": 423000,
     *       "non-actives": 7000,
     *       "check-logs": 900000
     *     }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryStateInfoByYear(String date) {
		String result = "";
		
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		for(int i = 1; i <= 12; i++) {
			if (i < 10) {
				String month = "0" + i;
				String statsKey = date + "-" + month;
				String queryDate = date + month;
				Map<String, Integer> statsValue = queryStateMapInfoByMonth(queryDate);
				statsMap.put(statsKey, statsValue);
			} else {
				String month = (new Integer(i)).toString();
				String statsKey = date + "-" + month;
				String queryDate = date + month;
				Map<String, Integer> statsValue = queryStateMapInfoByMonth(queryDate);
				statsMap.put(statsKey, statsValue);
			}
		}
		Gson gson = new Gson();
		
		//获取年的综合统计信息。
		Map<String, Integer> totalInfo = queryStateMapInfoByYear(date);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
}
