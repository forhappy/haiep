package cn.iie.haiep.hbase.query;

import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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

public class QueryProvinceInfo extends HaiepQuery {
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
	 * 查询省级日网吧数量。（不包含当天）
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级日网吧数量。（不包含当天）
	 */
	static public int queryNumOfNetbarsByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_TRADER[0], Constants.PROVINCE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级日网吧新增数量。
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级日网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_TRADER[0], Constants.PROVINCE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级日代理商数量。（不包含当天）
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级日代理商数量。（不包含当天）
	 */
	static public int queryNumOfAgentsByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_AGENT[0], Constants.PROVINCE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级日代理商新曾数量。
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级日代理商新曾数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_AGENT[0], Constants.PROVINCE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级活跃网吧数量。(不包含当天)
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级活跃网吧数量。(不包含当天)
	 */
	static public int queryNumOfActiveNetbarsByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级非活跃网吧数量。
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级日核查数量。
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级日核查数量。
	 */
	static public int queryNumOfCheckLogsByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_CHECK_NUMBER[0], Constants.PROVINCE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级上网人数。
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级上网人数。
	 */
	static public int queryNumOfPopulationByDay(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_SURF_PEOPLE_NUMBER[0], Constants.PROVINCE_SURF_PEOPLE_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月网吧数量。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月网吧数量。
	 */
	static public int queryNumOfNetbarsByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_TRADER[0], Constants.PROVINCE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月网吧新增数量。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_TRADER[0], Constants.PROVINCE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月代理商数量。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月代理商数量。
	 */
	static public int queryNumOfAgentsByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_AGENT[0], Constants.PROVINCE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月代理商新曾数量。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月代理商新曾数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_AGENT[0], Constants.PROVINCE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月活跃网吧数量。
	 * @param date 日期格式20120101
	 * @param areaCode 省区代码
	 * @return 省级月活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月非活跃网吧数量。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月核查数量。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月核查数量。
	 */
	static public int queryNumOfCheckLogsByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_CHECK_NUMBER[0], Constants.PROVINCE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级月上网人数。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 省级月上网人数。
	 */
	static public int queryNumOfPopulationByMonth(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_SURF_PEOPLE_NUMBER[0], Constants.PROVINCE_SURF_PEOPLE_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度网吧数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度网吧数量。
	 */
	static public int queryNumOfNetbarsByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_TRADER[0], Constants.PROVINCE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度网吧新增数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_TRADER[0], Constants.PROVINCE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度代理商数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度代理商数量。
	 */
	static public int queryNumOfAgentsByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_AGENT[0], Constants.PROVINCE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度代理商新曾数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度代理商新曾数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_AGENT[0], Constants.PROVINCE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度活跃网吧数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度非活跃网吧数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度核查数量。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度核查数量。
	 */
	static public int queryNumOfCheckLogsByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_CHECK_NUMBER[0], Constants.PROVINCE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级季度上网人数。
	 * @param date 日期格式2012X(X表述季度，如1表示第一季度，以此类推)
	 * @param areaCode 省区代码
	 * @return 省级季度上网人数。
	 */
	static public int queryNumOfPopulationByQuarter(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_SURF_PEOPLE_NUMBER[0], Constants.PROVINCE_SURF_PEOPLE_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年网吧数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年网吧数量。
	 */
	static public int queryNumOfNetbarsByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_TRADER[0], Constants.PROVINCE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年网吧新增数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_TRADER[0], Constants.PROVINCE_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年代理商数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年代理商数量。
	 */
	static public int queryNumOfAgentsByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_AGENT[0], Constants.PROVINCE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年代理商新增数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年代理商新增数量。
	 */
	static public int queryNumOfAgentsNewlyAddedByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_INCRE_AGENT[0], Constants.PROVINCE_INCRE_AGENT[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年活跃网吧数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年非活跃网吧数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[0], Constants.PROVINCE_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年核查数量。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年核查数量。
	 */
	static public int queryNumOfCheckLogsByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_CHECK_NUMBER[0], Constants.PROVINCE_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级年上网人数。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 省级年上网人数。
	 */
	static public int queryNumOfPopulationByYear(String date, String areaCode) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_PROVINCE_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.PROVINCE_SURF_PEOPLE_NUMBER[0], Constants.PROVINCE_SURF_PEOPLE_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(areaCode + date)));
		
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
	 * 查询省级日统计信息。
	 * @param date 日期格式20120508
	 * @return 返回 json 格式的全国日统计信息 。
	 * 
	 * {
	 *     "湖南": {
	 *       "area-code": 430000,
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "浙江": {
	 *       "area-code": 330000,
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "山东": {
	 *       "area-code": 370000,
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     ...
     *     "海南": {
	 *       "area-code": 460000,
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     "total": {
     *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryProvinceInfoByDay(String date) {
		String result = "";
		int totalPopulation = 0;
		String ppKey = "population";
		Iterator iterProvinces = Constants.PROVINCE_CODE_NAME_MAPPING.entrySet().iterator();
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		while (iterProvinces.hasNext()) {
			Entry entry = (Entry) iterProvinces.next();
			String areaCode = (String) entry.getKey();
			String provinceName = (String) entry.getValue();
			Map<String, Integer> provinceStatMap = queryProvinceMapInfoByDay(date, areaCode);
			//统计省级上网人数。
			totalPopulation = totalPopulation + provinceStatMap.get(ppKey);
			statsMap.put(provinceName, provinceStatMap);
		}
		Gson gson = new Gson();
		//直接获取全国的日综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByDayForProvinceStats(date);
		totalInfo.put(ppKey, new Integer(totalPopulation));
		
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询省级日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的省级统计信息 。
	 */
	static private Map<String, Integer> queryProvinceMapInfoByDay(String date, String areaCode) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		//areaCode 转换成整数；
		statsMap.put("area-code", new Integer(areaCode));
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date, areaCode)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date, areaCode)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByDay(date, areaCode)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByDay(date, areaCode)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date, areaCode)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date, areaCode)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date, areaCode)));
		statsMap.put("population", new Integer(queryNumOfPopulationByDay(date, areaCode)));

		return statsMap;
	}
	
	/**
	 * 查询省级月统计信息。
	 * @param date 日期格式201201
	 * @param areaCode 省区代码
	 * @return 返回 json 格式的省级月统计信息 。
	 * 
	 * {
	 *     "area-code": {"460000": "海南"},
	 *     "2012-04-01": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "2012-04-02": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     ...
	 *     "2012-04-30": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *   "total": {
     *     "netbars": 43000,
     *     "netbars-added": 2000,
     *     "agents": 800,
     *     "agents-added": 50,
     *     "actives": 423000,
     *     "non-actives": 7000,
     *     "check-logs": 900000,
     *     "population":50000
     *   }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryProvinceInfoByMonth(String date, String areaCode) {
		String result = "";
		
		String year = date.substring(0, 4);
		String month = date.substring(4, 6);
		Calendar cal=Calendar.getInstance();
		cal.clear();
		cal.set(Calendar.YEAR, new Integer(year));
		cal.set(Calendar.MONTH, new Integer(month) - 1);
		int days = cal.getActualMaximum(Calendar.DAY_OF_MONTH);//本月份的天数;
		
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		
		Map<String, String> areaCodeMap = new LinkedHashMap<String, String>();
		areaCodeMap.put(areaCode, Constants.PROVINCE_CODE_NAME_MAPPING.get(areaCode));
		statsMap.put("area-code", areaCodeMap);
		for(int i = 1; i <= days; i++) {
			if (i < 10) {
				String day = "0" + i;
				String dateStatsKey = year + "-" + month + "-" + day;
				String queryDate = year + month + day;
				Map<String, Integer> dateStatsValue = queryProvinceMapInfoByDay(queryDate, areaCode);
				statsMap.put(dateStatsKey, dateStatsValue);
			} else {
				String day = (new Integer(i)).toString();
				String dateStatsKey = year + "-" + month + "-" + day;
				String queryDate = year + month + day;
				Map<String, Integer> dateStatsValue = queryProvinceMapInfoByDay(queryDate, areaCode);
				statsMap.put(dateStatsKey, dateStatsValue);
			}
		}
		Gson gson = new Gson();
		//获取月份的综合统计信息。
		Map<String, Integer> totalInfo = queryProvinceMapInfoByMonth(date, areaCode);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		
		
		return result;
	}
	
	/**
	 * 查询省级月统计信息。
	 * @param date 日期格式201205
	 * @return 返回 json 格式的省级月统计信息 。
	 * 
	 * {
	 *     "湖南": {
	 *       "area-code": 430000,
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "浙江": {
	 *       "area-code": 330000,
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "山东": {
	 *       "area-code": 370000,
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     ...
     *     "海南": {
	 *       "area-code": 460000,
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     "total": {
     *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryProvinceInfoByMonth(String date) {
		String result = "";
		int totalPopulation = 0;
		String ppKey = "population";
		Iterator iterProvinces = Constants.PROVINCE_CODE_NAME_MAPPING.entrySet().iterator();
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		while (iterProvinces.hasNext()) {
			Entry entry = (Entry) iterProvinces.next();
			String areaCode = (String) entry.getKey();
			String provinceName = (String) entry.getValue();
			Map<String, Integer> provinceStatMap = queryProvinceMapInfoByMonth(date, areaCode);
			//统计省级上网人数。
			totalPopulation = totalPopulation + provinceStatMap.get(ppKey);
			statsMap.put(provinceName, provinceStatMap);
		}
		Gson gson = new Gson();
		//直接获取全国的月综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByMonthForProvinceStats(date);
		totalInfo.put(ppKey, new Integer(totalPopulation));
		
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询省级月统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的省级统计信息 。
	 */
	static private Map<String, Integer> queryProvinceMapInfoByMonth(String date, String areaCode) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("area-code", new Integer(areaCode));
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date, areaCode)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date, areaCode)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByMonth(date, areaCode)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByMonth(date, areaCode)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date, areaCode)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date, areaCode)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date, areaCode)));
		statsMap.put("population", new Integer(queryNumOfPopulationByMonth(date, areaCode)));

		return statsMap;
	}
	
	/**
	 * 查询全国季度统计信息。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 返回 json 格式的全国季度统计信息 。
	 * 
	 * {
	 *     "area-code": {"460000": "海南"},
	 *     "第1季度": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "第2季度": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "第3季度": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     "第4季度": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryProvinceInfoByQuater(String date, String areaCode) {
		String result = "";
		Map<String, Map>  quarterStatsMap = new LinkedHashMap<String, Map>();
		
		Map<String, String> areaCodeMap = new LinkedHashMap<String, String>();
		areaCodeMap.put(areaCode, Constants.PROVINCE_CODE_NAME_MAPPING.get(areaCode));
		quarterStatsMap.put("area-code", areaCodeMap);
		
		for(int i = 1; i <= 4; i++) {
				String statsKey = "第" + i + "季度";
				String queryDate = date + i;
				Map<String, Integer> statsValue = queryProvinceMapInfoByQuarter(queryDate, areaCode);
				quarterStatsMap.put(statsKey, statsValue);
		}
		Gson gson = new Gson();
		result = gson.toJson(quarterStatsMap);
		return result;
	}
	
	/**
	 * 查询省级季度统计信息。
	 * @param date 日期格式2012X(X表示季度，1表示第1季度，2表示第2季度等)
	 * @return 返回 Map 格式的省级统计信息 。

	 */
	static private Map<String, Integer> queryProvinceMapInfoByQuarter(String date, String areaCode) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByQuarter(date, areaCode)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByQuarter(date, areaCode)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByQuarter(date, areaCode)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByQuarter(date, areaCode)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByQuarter(date, areaCode)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByQuarter(date, areaCode)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByQuarter(date, areaCode)));
		statsMap.put("population", new Integer(queryNumOfPopulationByQuarter(date, areaCode)));

		return statsMap;
	}
	
	/**
	 * 查询省级年统计信息。
	 * @param date 日期格式2012
	 * @param areaCode 省区代码
	 * @return 返回 json 格式的全国统计信息 。
	 * 
	 * {
	 *     "area-code": {"460000": "海南"},
	 *     "2012-01": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "2012-02": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     ...
	 *     "2012-12": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     "total": {
     *       "netbars": 43000,
     *       "netbars-added": 2000,
     *       "agents": 800,
     *       "agents-added": 50,
     *       "actives": 423000,
     *       "non-actives": 7000,
     *       "check-logs": 900000,
     *       "population":50000
     *     }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryProvinceInfoByYear(String date, String areaCode) {
		String result = "";
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		
		Map<String, String> areaCodeMap = new LinkedHashMap<String, String>();
		areaCodeMap.put(areaCode, Constants.PROVINCE_CODE_NAME_MAPPING.get(areaCode));
		statsMap.put("area-code", areaCodeMap);
		
		for(int i = 1; i <= 12; i++) {
			if (i < 10) {
				String month = "0" + i;
				String statsKey = date + "-" + month;
				String queryDate = date + month;
				Map<String, Integer> statsValue = queryProvinceMapInfoByMonth(queryDate, areaCode);
				statsMap.put(statsKey, statsValue);
			} else {
				String month = (new Integer(i)).toString();
				String statsKey = date + "-" + month;
				String queryDate = date + month;
				Map<String, Integer> statsValue = queryProvinceMapInfoByMonth(queryDate, areaCode);
				statsMap.put(statsKey, statsValue);
			}
		}
		Gson gson = new Gson();
		
		//获取年的综合统计信息。
		Map<String, Integer> totalInfo = queryProvinceMapInfoByYear(date, areaCode);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询省级年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 json 格式的省级年统计信息 。
	 * 
	 * {
	 *     "湖南": {
	 *       "area-code": 430000,
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "浙江": {
	 *       "area-code": 330000,
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "agents": 80,
	 *       "agents-added": 5,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000,
	 *       "population":50000
	 *     },
	 *     "山东": {
	 *       "area-code": 370000,
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     ...
     *     "海南": {
	 *       "area-code": 460000,
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     },
     *     "total": {
     *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "agents": 80,
     *       "agents-added": 5,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000,
     *       "population":50000
     *     }
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryProvinceInfoByYear(String date) {
		String result = "";
		int totalPopulation = 0;
		String ppKey = "population";
		Iterator iterProvinces = Constants.PROVINCE_CODE_NAME_MAPPING.entrySet().iterator();
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		while (iterProvinces.hasNext()) {
			Entry entry = (Entry) iterProvinces.next();
			String areaCode = (String) entry.getKey();
			String provinceName = (String) entry.getValue();
			Map<String, Integer> provinceStatMap = queryProvinceMapInfoByYear(date, areaCode);
			//统计省级上网人数。
			totalPopulation = totalPopulation + provinceStatMap.get(ppKey);
			statsMap.put(provinceName, provinceStatMap);
		}
		Gson gson = new Gson();
		//直接获取全国的年综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByYearForProvinceStats(date);
		totalInfo.put(ppKey, new Integer(totalPopulation));
		
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询省级年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的省级统计信息 。

	 */
	static private Map<String, Integer> queryProvinceMapInfoByYear(String date, String areaCode) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("area-code", new Integer(areaCode));
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date, areaCode)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date, areaCode)));
		statsMap.put("agents", new Integer(queryNumOfAgentsByYear(date, areaCode)));
		statsMap.put("agents-added", new Integer(queryNumOfAgentsNewlyAddedByYear(date, areaCode)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date, areaCode)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date, areaCode)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date, areaCode)));
		statsMap.put("population", new Integer(queryNumOfPopulationByYear(date, areaCode)));

		return statsMap;
	}
}
