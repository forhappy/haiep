package cn.iie.haiep.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;


public class QueryAgentInfo extends HaiepQuery {
	static public class AgentInfo {
		public AgentInfo() {
			genAgentID2NameMapping();
			iter = agentID2NameMapping.entrySet().iterator();
		}
		static final String agentTableName = "HAIEP.AGENT_INFO";
		/**
		 * agent id to name mapping.
		 */
		private Map<String, List<String>> agentID2NameMapping = new LinkedHashMap<String, List<String>>();
		@SuppressWarnings("rawtypes")
		private Iterator iter= null;
		
		public void genAgentID2NameMapping() {
			HTablePool pool = new HTablePool(configuration, 1000);
			HTable table = (HTable) pool.getTable(agentTableName);
			try {
				ResultScanner rs = table.getScanner(new Scan());
				for (Result r : rs) {
					String agentID = "";
					String agentCode = "";
					String agentName = "";
					List<String> codeAndName = new ArrayList<String>();
					for (KeyValue keyValue : r.raw()) {
						String qualifier = new String(keyValue.getQualifier());
						if (qualifier.equalsIgnoreCase("AGENT_ID")) {
							agentID = new String(keyValue.getValue());
						}
						if (qualifier.equalsIgnoreCase("AGENT_CODE")) {
							agentCode = new String(keyValue.getValue());
						}
						if (qualifier.equalsIgnoreCase("AGENT_NAME")) {
							agentName = new String(keyValue.getValue());
						}
					}
					codeAndName.add(agentCode);
					codeAndName.add(agentName);
					agentID2NameMapping.put(agentID, codeAndName);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public List<String> getAgentNameByID(String agentID) {
			return agentID2NameMapping.get(agentID);
		}
		
		public Boolean hasNext() {
			return iter.hasNext();
		}
		
		@SuppressWarnings("rawtypes")
		public Entry next() {
			return (Entry) iter.next();
		}
	}
	
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
	 * 查询代理商日网吧数量。（不包含当天）
	 * @param date 日期格式20120101
	 * @param agentID 代理商代码
	 * @return 代理商日网吧数量。（不包含当天）
	 */
	static public int queryNumOfNetbarsByDay(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_TRADER[0], Constants.AGENT_TRADER[1]);
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商日网吧新增数量。
	 * @param date 日期格式20120101
	 * @param agentID 代理商代码
	 * @return 代理商日网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByDay(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_INCRE_TRADER[0], Constants.AGENT_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商活跃网吧数量。(不包含当天)
	 * @param date 日期格式20120101
	 * @param agentID 代理商代码
	 * @return 代理商活跃网吧数量。(不包含当天)
	 */
	static public int queryNumOfActiveNetbarsByDay(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_ACTIVE_TRADER[0], Constants.AGENT_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商非活跃网吧数量。
	 * @param date 日期格式20120101
	 * @param agentID 代理商代码
	 * @return 代理商非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByDay(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_NON_ACTIVE_TRADER[0], Constants.AGENT_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商日核查数量。
	 * @param date 日期格式20120101
	 * @param agentID 代理商代码
	 * @return 代理商日核查数量。
	 */
	static public int queryNumOfCheckLogsByDay(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_CHECK_NUMBER[0], Constants.AGENT_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商月网吧数量。
	 * @param date 日期格式201201
	 * @param agentID 代理商代码
	 * @return 代理商月网吧数量。
	 */
	static public int queryNumOfNetbarsByMonth(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_TRADER[0], Constants.AGENT_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商月网吧新增数量。
	 * @param date 日期格式201201
	 * @param agentID 代理商代码
	 * @return 代理商月网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByMonth(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_INCRE_TRADER[0], Constants.AGENT_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商月活跃网吧数量。
	 * @param date 日期格式20120101
	 * @param agentID 代理商代码
	 * @return 代理商月活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByMonth(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_ACTIVE_TRADER[0], Constants.AGENT_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商月非活跃网吧数量。
	 * @param date 日期格式201201
	 * @param agentID 代理商代码
	 * @return 代理商月非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByMonth(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_NON_ACTIVE_TRADER[0], Constants.AGENT_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商月核查数量。
	 * @param date 日期格式201201
	 * @param agentID 代理商代码
	 * @return 代理商月核查数量。
	 */
	static public int queryNumOfCheckLogsByMonth(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.MONTH_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_CHECK_NUMBER[0], Constants.AGENT_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商季度网吧数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商季度网吧数量。
	 */
	static public int queryNumOfNetbarsByQuarter(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_TRADER[0], Constants.AGENT_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商季度网吧新增数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商季度网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByQuarter(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_INCRE_TRADER[0], Constants.AGENT_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商季度活跃网吧数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商季度活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByQuarter(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_ACTIVE_TRADER[0], Constants.AGENT_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商季度非活跃网吧数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商季度非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByQuarter(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_NON_ACTIVE_TRADER[0], Constants.AGENT_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商季度核查数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商季度核查数量。
	 */
	static public int queryNumOfCheckLogsByQuarter(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.QUARTER_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_CHECK_NUMBER[0], Constants.AGENT_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商年网吧数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商年网吧数量。
	 */
	static public int queryNumOfNetbarsByYear(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_TRADER[0], Constants.AGENT_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商年网吧新增数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商年网吧新增数量。
	 */
	static public int queryNumOfNetbarsNewlyAddedByYear(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_INCRE_TRADER[0], Constants.AGENT_INCRE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商年活跃网吧数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商年活跃网吧数量。
	 */
	static public int queryNumOfActiveNetbarsByYear(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_ACTIVE_TRADER[0], Constants.AGENT_DAILY_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商年非活跃网吧数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商年非活跃网吧数量。
	 */
	static public int queryNumOfNonActiveNetbarsByYear(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_DAILY_NON_ACTIVE_TRADER[0], Constants.AGENT_DAILY_NON_ACTIVE_TRADER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商年核查数量。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 代理商年核查数量。
	 */
	static public int queryNumOfCheckLogsByYear(String date, String agentID) {
		HTablePool pool = new HTablePool(configuration, 1024);
		HTable table = (HTable) pool.getTable(Constants.YEAR_AGENT_STAT_NAME);
		
		int result = 0;
		
		Scan scan = new Scan();
		scan.addColumn(Constants.AGENT_CHECK_NUMBER[0], Constants.AGENT_CHECK_NUMBER[1]);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes(agentID + date)));
		
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
	 * 查询代理商日统计信息。
	 * @param date 日期格式20120508
	 * @return 返回 json 格式的代理商日统计信息 。
	 * 
	 * {
	 *     "123456": {
	 *       "agent-id": "232324",
	 *       "agent-name": "代理商1",
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "123457": {
	 *       "agent-id": "231334",
	 *       "agent-name": "代理商2",
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     ...
     *     "123458": {
     *       "agent-id": "231334",
     *       "agent-name": "代理商3",
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "total": {
     *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     }
     * }
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static public String queryAgentInfoByDay(String date) {
		String result = "";
		
		AgentInfo agentInfo = new AgentInfo();
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		while (agentInfo.hasNext()) {
			Entry entry = agentInfo.next();
			
			
			String agentID = (String) entry.getKey();
			List<String> codeAndName = (ArrayList<String>) entry.getValue();
			String agentCode = codeAndName.get(0);
			String agentName = codeAndName.get(1);
			
			Map<String, Object> agentStatMap = queryAgentMapInfoByDay(date, agentID, agentCode, agentName);
			statsMap.put(agentCode, agentStatMap);
		}
		Gson gson = new Gson();
		//直接获取全国日综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByDayForAgentStats(date);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询代理商日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	static private Map<String, Object> queryAgentMapInfoByDay(String date, String agentID) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	@SuppressWarnings("unused")
	static private Map<String, Object> queryAgentMapInfoByDay(String date, String agentID, String agentName) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("agent-name", agentName);
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	static private Map<String, Object> queryAgentMapInfoByDay(String date, String agentID, String agentCode, String agentName) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("agent-id", agentID);
		statsMap.put("agent-name", agentName);
		
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByDay(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByDay(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByDay(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByDay(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByDay(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商月统计信息。
	 * @param date 日期格式201201
	 * @param agentID 代理商代码
	 * @return 返回 json 格式的代理商月统计信息 。
	 * 
	 * {
	 *   "2012-04-01": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "2012-04-02": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     ...
	 *     "2012-04-30": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *   "total": {
     *     "netbars": 43000,
     *     "netbars-added": 2000,
     *     "actives": 423000,
     *     "non-actives": 7000,
     *     "check-logs": 900000
     *   }}
	 */
	@SuppressWarnings("rawtypes")
	static public String queryAgentInfoByMonth(String date, String agentID) {
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
				String dateStatsKey = year + "-" + month + "-" + day;
				String queryDate = year + month + day;
				Map<String, Object> dateStatsValue = queryAgentMapInfoByDay(queryDate, agentID);
				statsMap.put(dateStatsKey, dateStatsValue);
			} else {
				String day = (new Integer(i)).toString();
				String dateStatsKey = year + "-" + month + "-" + day;
				String queryDate = year + month + day;
				Map<String, Object> dateStatsValue = queryAgentMapInfoByDay(queryDate, agentID);
				statsMap.put(dateStatsKey, dateStatsValue);
			}
		}
		Gson gson = new Gson();
		//获取月份的综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByDayForAgentStats(date);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询代理商月统计信息。
	 * @param date 日期格式201205
	 * @return 返回 json 格式的代理商月统计信息 。
	 * 
	 * {
	 *     "123456": {
	 *       "agent-id": "232334",
	 *       "agent-name": "代理商1",
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "123457": {
	 *       "agent-id": "232332",
	 *       "agent-name": "代理商2",
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     ...
     *     "123458": {
     *       "agent-id": "232334",
     *       "agent-name": "代理商3",
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "total": {
     *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     }
     * }
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static public String queryAgentInfoByMonth(String date) {
		String result = "";
		
		AgentInfo agentInfo = new AgentInfo();
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		while (agentInfo.hasNext()) {
			Entry entry = agentInfo.next();
			
			String agentID = (String) entry.getKey();
			List<String> codeAndName = (ArrayList<String>) entry.getValue();
			String agentCode = codeAndName.get(0);
			String agentName = codeAndName.get(1);
//			String agentID = (String) entry.getKey();
//			String agentName = (String) entry.getValue();
			
			Map<String, Object> agentStatMap = queryAgentMapInfoByMonth(date, agentID, agentCode, agentName);
			statsMap.put(agentCode, agentStatMap);
		}
		Gson gson = new Gson();
		//直接获取全国日综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByMonthForAgentStats(date);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询省级日统计信息。
	 * @param date 日期格式20120101
	 * @return 返回 Map 格式的省级统计信息 。
	 */
	static private Map<String, Integer> queryAgentMapInfoByMonth(String date, String agentID) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date, agentID)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date, agentID)));

		return statsMap;
	}
	
	
	
	/**
	 * 查询代理商月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	@SuppressWarnings("unused")
	static private Map<String, Object> queryAgentMapInfoByMonth(String date, String agentID, String agentName) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("agent-name", agentName);
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商月统计信息。
	 * @param date 日期格式201201
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	static private Map<String, Object> queryAgentMapInfoByMonth(String date, String agentID, String agentCode, String agentName) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("agent-id", agentID);
		statsMap.put("agent-name", agentName);
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByMonth(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByMonth(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByMonth(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByMonth(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByMonth(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商季度统计信息。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 返回 json 格式的代理商季度统计信息 。
	 * 
	 * {
	 *   "第1季度": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "第2季度": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "第3季度": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "第4季度": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     * }
	 */
	@SuppressWarnings("rawtypes")
	static public String queryAgentInfoByQuater(String date, String agentID) {
		String result = "";
		
		Map<String, Map>  quarterStatsMap = new LinkedHashMap<String, Map>();

		for(int i = 1; i <= 4; i++) {
				String statsKey = "第" + i + "季度";
				String queryDate = date + i;
				Map<String, Integer> statsValue = queryAgentMapInfoByQuarter(queryDate, agentID);
				quarterStatsMap.put(statsKey, statsValue);
		}
		Gson gson = new Gson();
		result = gson.toJson(quarterStatsMap);
		return result;
	}
	
	/**
	 * 查询代理商季度统计信息。
	 * @param date 日期格式2012X(X表示季度，1表示第1季度，2表示第2季度等)
	 * @return 返回 Map 格式的代理商统计信息 。

	 */
	static private Map<String, Integer> queryAgentMapInfoByQuarter(String date, String agentID) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByQuarter(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByQuarter(date, agentID)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByQuarter(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByQuarter(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByQuarter(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商年统计信息。
	 * @param date 日期格式2012
	 * @param agentID 代理商代码
	 * @return 返回 json 格式的代理商统计信息 。
	 * 
	 * {
	 *     "2012-01": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "2012-02": {
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     ...
	 *     "2012-12": {
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *   "total": {
     *     "netbars": 43000,
     *     "netbars-added": 2000,
     *     "actives": 423000,
     *     "non-actives": 7000,
     *     "check-logs": 900000
     *   }}
	 */
	@SuppressWarnings("rawtypes")
	static public String queryAgentInfoByYear(String date, String agentID) {
		String result = "";
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		
		for(int i = 1; i <= 12; i++) {
			if (i < 10) {
				String month = "0" + i;
				String dateStatsKey = date + "-" + month;
				String queryDate = date + month;
				Map<String, Integer> dateStatsValue = queryAgentMapInfoByMonth(queryDate, agentID);
				statsMap.put(dateStatsKey, dateStatsValue);
			} else {
				String month = (new Integer(i)).toString();
				String dateStatsKey = date + "-" + month;
				String queryDate = date + month;
				Map<String, Integer> dateStatsValue = queryAgentMapInfoByMonth(queryDate, agentID);
				statsMap.put(dateStatsKey, dateStatsValue);
			}
		}
		Gson gson = new Gson();
		
		//获取年的综合统计信息。
		Map<String, Integer> totalInfo = queryAgentMapInfoByYear(date, agentID);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	/**
	 * 查询代理商年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 json 格式的代理商年统计信息 。
	 * 
	 * {
	 *     "123456": {
	 *       "agent-id": "232334",
	 *       "agent-name": "代理商1",
	 *       "netbars": 4300,
	 *       "netbars-added": 200,
	 *       "actives": 42300,
	 *       "non-actives": 700,
	 *       "check-logs": 90000
	 *     },
	 *     "123457": {
	 *       "agent-id": "232334",
	 *       "agent-name": "代理商2",
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     ...
     *     "123458": {
     *       "agent-id": "232334",
     *       "agent-name": "代理商3",
	 *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     },
     *     "total": {
     *       "netbars": 4300,
     *       "netbars-added": 200,
     *       "actives": 42300,
     *       "non-actives": 700,
     *       "check-logs": 90000
     *     }
     * }
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static public String queryAgentInfoByYear(String date) {
		String result = "";
		
		AgentInfo agentInfo = new AgentInfo();
		Map<String, Map> statsMap = new LinkedHashMap<String, Map>();
		while (agentInfo.hasNext()) {
			Entry entry = agentInfo.next();
			
			String agentID = (String) entry.getKey();
			List<String> codeAndName = (ArrayList<String>) entry.getValue();
			String agentCode = codeAndName.get(0);
			String agentName = codeAndName.get(1);
//			String agentID = (String) entry.getKey();
//			String agentName = (String) entry.getValue();
			
			Map<String, Object> agentStatMap = queryAgentMapInfoByYear(date, agentID, agentCode, agentName);
			statsMap.put(agentCode, agentStatMap);
		}
		Gson gson = new Gson();
		//直接获取全国日综合统计信息。
		Map<String, Integer> totalInfo = QueryStateInfo.queryStateMapInfoByYearForAgentStats(date);
		statsMap.put("total", totalInfo);
		
		result = gson.toJson(statsMap);
		return result;
	}
	
	
	/**
	 * 查询全国年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的全国统计信息 。

	 */
	static private Map<String, Integer> queryAgentMapInfoByYear(String date, String agentID) {
		Map<String, Integer> statsMap = new LinkedHashMap<String, Integer>();
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date, agentID)));
		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	@SuppressWarnings("unused")
	static private Map<String, Object> queryAgentMapInfoByYear(String date, String agentID, String agentName) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("agent-name", agentName);
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date, agentID)));

		return statsMap;
	}
	
	/**
	 * 查询代理商年统计信息。
	 * @param date 日期格式2012
	 * @return 返回 Map 格式的代理商统计信息 。
	 */
	static private Map<String, Object> queryAgentMapInfoByYear(String date, String agentID, String agentCode, String agentName) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("agent-id", agentID);
		statsMap.put("agent-name", agentName);
		statsMap.put("netbars", new Integer(queryNumOfNetbarsByYear(date, agentID)));
		statsMap.put("netbars-added", new Integer(queryNumOfNetbarsNewlyAddedByYear(date, agentID)));

		statsMap.put("actives", new Integer(queryNumOfActiveNetbarsByYear(date, agentID)));
		statsMap.put("non-actives", new Integer(queryNumOfNonActiveNetbarsByYear(date, agentID)));
		statsMap.put("check-logs", new Integer(queryNumOfCheckLogsByYear(date, agentID)));

		return statsMap;
	}
}
