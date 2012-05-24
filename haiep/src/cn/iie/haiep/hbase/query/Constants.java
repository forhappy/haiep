package cn.iie.haiep.hbase.query;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Constants {
	// TODO trader check details
	// TODO agent active/non-active trader details

	/**
	 * Table name prefix indicating where are we on time dimension
	 * 
	 * THE FIRST ONE INDICATING DAY MUST BE EMPTY!!!
	 */

	/**
	 * trader statistics table schema
	 */
	// total number of check details
	public static final byte[][] TRADER_CHECK_NUMBER = { "c".getBytes(),
			"t".getBytes() };

	// trader statistics table
	public static final byte[][][] TRADER_STAT = { TRADER_CHECK_NUMBER };

	public static final String TRADER_STAT_NAME = "trader_stat";

	public static final String MONTH_TRADER_STAT_NAME = "month_trader_stat";

	public static final String QUARTER_TRADER_STAT_NAME = "quarter_trader_stat";

	public static final String YEAR_TRADER_STAT_NAME = "year_trader_stat";
	/**
	 * agent statistics table schema
	 */
	public static final byte[][] AGENT_TRADER = { "s".getBytes(),
			"t".getBytes() };// s for secondary means it need another step to be
								// calculated

	public static final byte[][] AGENT_INCRE_TRADER = { "t".getBytes(),
			"i".getBytes() };

	public static final byte[][] AGENT_DAILY_ACTIVE_TRADER = { "c".getBytes(),
			"a".getBytes() };

	public static final byte[][] AGENT_DAILY_NON_ACTIVE_TRADER = {
			"s".getBytes(), "na".getBytes() };

	public static final byte[][] AGENT_CHECK_NUMBER = { "c".getBytes(),
			"n".getBytes() };

	public static final byte[][][] AGENT_STAT = { AGENT_TRADER,
			AGENT_INCRE_TRADER, AGENT_DAILY_ACTIVE_TRADER,
			AGENT_DAILY_NON_ACTIVE_TRADER, AGENT_CHECK_NUMBER };

	public static final String AGENT_STAT_NAME = "agent_stat";

	public static final String MONTH_AGENT_STAT_NAME = "month_agent_stat";

	public static final String QUARTER_AGENT_STAT_NAME = "quarter_agent_stat";

	public static final String YEAR_AGENT_STAT_NAME = "year_agent_stat";

	/**
	 * province statistics table schema
	 */

	public static final byte[][] PROVINCE_TRADER = { "s".getBytes(),
			"t".getBytes() };

	public static final byte[][] PROVINCE_INCRE_TRADER = { "t".getBytes(),
			"i".getBytes() };

	public static final byte[][] PROVINCE_AGENT = { "s".getBytes(),
			"a".getBytes() };

	public static final byte[][] PROVINCE_INCRE_AGENT = { "a".getBytes(),
			"i".getBytes() };

	public static final byte[][] PROVINCE_DAILY_ACTIVE_TRADER = {
			"c".getBytes(), "a".getBytes() };

	public static final byte[][] PROVINCE_DAILY_NON_ACTIVE_TRADER = {
			"s".getBytes(), "na".getBytes() };

	public static final byte[][] PROVINCE_CHECK_NUMBER = { "c".getBytes(),
			"n".getBytes() };

	public static final byte[][] PROVINCE_SURF_PEOPLE_NUMBER = {
			"p".getBytes(), "n".getBytes() };

	public static final byte[][][] PROVINCE_STAT = { PROVINCE_TRADER,
			PROVINCE_INCRE_TRADER, PROVINCE_AGENT, PROVINCE_INCRE_AGENT,
			PROVINCE_DAILY_ACTIVE_TRADER, PROVINCE_DAILY_NON_ACTIVE_TRADER,
			PROVINCE_CHECK_NUMBER, PROVINCE_SURF_PEOPLE_NUMBER };

	public static final String PROVINCE_STAT_NAME = "province_stat";

	public static final String MONTH_PROVINCE_STAT_NAME = "month_province_stat";

	public static final String QUARTER_PROVINCE_STAT_NAME = "quarter_province_stat";

	public static final String YEAR_PROVINCE_STAT_NAME = "year_province_stat";

	/**
	 * state statistics table schema
	 */

	public static final byte[][] STATE_TRADER = { "t".getBytes(),
			"t".getBytes() };

	public static final byte[][] STATE_INCRE_TRADER = { "t".getBytes(),
			"i".getBytes() };

	public static final byte[][] STATE_AGENT = { "a".getBytes(), "t".getBytes() };

	public static final byte[][] STATE_INCRE_AGENT = { "a".getBytes(),
			"i".getBytes() };

	public static final byte[][] STATE_DAILY_ACTIVE_TRADER = { "c".getBytes(),
			"a".getBytes() };

	public static final byte[][] STATE_DAILY_NON_ACTIVE_TRADER = {
			"c".getBytes(), "na".getBytes() };

	public static final byte[][] STATE_CHECK_NUMBER = { "c".getBytes(),
			"n".getBytes() };

	public static final byte[][][] STATE_STAT = { STATE_TRADER,
			STATE_INCRE_TRADER, STATE_AGENT, STATE_INCRE_AGENT,
			STATE_DAILY_ACTIVE_TRADER, STATE_DAILY_ACTIVE_TRADER,
			STATE_DAILY_NON_ACTIVE_TRADER, STATE_CHECK_NUMBER };

	public static final String STATE_STAT_NAME = "state_stat";

	public static final String MONTH_STATE_STAT_NAME = "month_state_stat";

	public static final String QUARTER_STATE_STAT_NAME = "quarter_state_stat";

	public static final String YEAR_STATE_STAT_NAME = "year_state_stat";

	/**
	 * primary check detail table;
	 */
	public static final byte[][] CD_TRADER_NAME = { "COMMONS".getBytes(),
			"T_TRADER_NAME".getBytes() };

	public static final byte[][] CD_AGENT_ID = { "COMMONS".getBytes(),
			"AGENT_ID".getBytes() };

	public static final byte[][] CD_AREA_CODE = { "COMMONS".getBytes(),
			"AREA_CODE".getBytes() };

	public static final byte[][] CD_CHECK_TIME = { "COMMONS".getBytes(),
			"CHECK_TIME".getBytes() };

	public static final byte[][] CD_ID_CARD = { "COMMONS".getBytes(),
			"IDCARD_NO".getBytes() };

	public static final String CD_TABLE_NAME = "HAIEP.CHECK_LOG";

	public static final byte[][] TT_CREATE_TIME = { "COMMONS".getBytes(),
			"CREATE_TIME".getBytes() };

	public static final byte[][] TT_AGENT_ID = { "COMMONS".getBytes(),
			"AGENT_ID".getBytes() };

	public static final byte[][] TT_AREA_CODE = { "COMMONS".getBytes(),
			"AREA_CODE".getBytes() };

	public static final String TT_TABLE_NAME = "HAIEP.TERMINAL_TRADER";

	public static final byte[][] AG_CREATE_TIME = { "COMMONS".getBytes(),
			"UPDATE_TIME".getBytes() };

	public static final byte[][] AG_AREA_ID = { "COMMONS".getBytes(),
			"AREA_ID".getBytes() };

	public static final String AG_TABLE_NAME = "HAIEP.AGENT_INFO";
	
	public static final byte[][] AREA_AREA_ID = {"COMMONS".getBytes(),
		"AREA_ID".getBytes() };
	
	public static final byte[][] AREA_AREA_CODE = {"COMMONS".getBytes(),
		"AREA_CODE".getBytes() };
	
	public static final String AREA_TABLE_NAME = "HAIEP.AREA_INFO";

	public static final String SUMUP_ARRAY_LENGTH = "sumup.array.length";
	public static final String SUMUP_REDUCER_HANDLER_CLASS_NAME = "sumup.reducer.handler.class.name";

	public static final String CHECK_JOB_MAP_KEY_GENERATOR = "checkjob.map.key.generator";
	public static final String CHECK_JOB_REDUCE_TRADER_RECORD_HANDLER = "checkjob.reduce.trader.record.handler";
	public static final String CHECK_JOB_REDUCE_AREA_RECORD_HANDLER = "checkjob.reduce.trader.area.handler";

	public static final String INCRE_JOB_MAP_KEY_GENERATOR = "increjob.map.key.generator";

	public static final String INCRE_JOB_REDUCE_COLUMN_NAME_GENERATOR = "increjob.reduce.column.name.generator";

	public static final String SEC_JOB_NAME_GENERATOR = "secjob.name.generator";
	
	public static final String SEC_JOB_NEED_CAL_NON_ACTIVE = "sec.job.need.cal.non.active";

	public static final String TIME_RU_JOB_MAP_KEY_GENERATOR = "timerujob.map.key.generator";

	public static final String TIME_RU_COLUMNS_GENERATOR = "timerujob.columns.generator";

	/**
	 * Configuration table schema
	 */
	public static final byte[][] CONFIG_VALUE = { "v".getBytes(), "".getBytes() };

	public static final byte[][][] CONFIG_TABLE = { CONFIG_VALUE };

	public static final String CONFIG_TABLE_NAME = "config_table";
	/**
	 * Configuration keys
	 */
	public static final byte[] CONFIG_LAST_IMPORT_TIME = "CONFIG_LAST_IMEPORT_TIME"
			.getBytes();
	
	public static enum TASKS{
		AGENT_CHECK_JOB,
		PROVINCE_CHECK_JOB,
		AGENT_TRADER_INCRE_JOB,
		PROVINCE_TRADER_INCRE_JOB,
		PROVINCE_AGENT_INCRE_JOB,
		AGENT_SEC_JOB,
		PROVINCE_SEC_JOB,
		STATE_JOB,
		PROVINCE_PEOPLE_JOB
	}
	
	public static final byte[][] CONFIG_LAST_TASKS_TIME = {
			"AGENT_CHECK_JOB".getBytes(),
			"PROVINCE_CHECK_JOB".getBytes(),
			"AGENT_TRADER_INCRE_JOB".getBytes(),
			"PROVINCE_TRADER_INCRE_JOB".getBytes(),
			"PROVINCE_AGENT_INCRE_JOB".getBytes(),
			"AGENT_SEC_JOB".getBytes(),
			"PROVINCE_SEC_JOB".getBytes(),
			"STATE_JOB".getBytes(),
			"PROVINCE_PEOPLE_JOB".getBytes(), 
	};
	/*Time roll up tasks' name will be (i,j) and won't be defined here*/
	
	
	enum TIME_FORMAT{
		YYYYMMMDD,YYYYMM,YYYYQ
	}
    
    public static final Map<String, String> PROVINCE_CODE_NAME_MAPPING = new LinkedHashMap<String, String>();
    
    public static final Set<String> TARGET_TALBE = new TreeSet<String>();
    static {
    	TARGET_TALBE.add("check_log");
    	TARGET_TALBE.add("agent_info");
    	TARGET_TALBE.add("terminal_trader");
    }

	static {
		PROVINCE_CODE_NAME_MAPPING.put("110000", "北京市");
		PROVINCE_CODE_NAME_MAPPING.put("120000", "天津市");
		PROVINCE_CODE_NAME_MAPPING.put("130000", "河北省");
		PROVINCE_CODE_NAME_MAPPING.put("140000", "山西省");
		PROVINCE_CODE_NAME_MAPPING.put("150000", "内蒙古自治区");
		PROVINCE_CODE_NAME_MAPPING.put("210000", "辽宁省");
		PROVINCE_CODE_NAME_MAPPING.put("220000", "吉林省");
		PROVINCE_CODE_NAME_MAPPING.put("230000", "黑龙江省");
		PROVINCE_CODE_NAME_MAPPING.put("310000", "上海市");
		PROVINCE_CODE_NAME_MAPPING.put("320000", "江苏省");
		PROVINCE_CODE_NAME_MAPPING.put("330000", "浙江省");
		PROVINCE_CODE_NAME_MAPPING.put("340000", "安徽省");
		PROVINCE_CODE_NAME_MAPPING.put("350000", "福建省");
		PROVINCE_CODE_NAME_MAPPING.put("360000", "江西省");
		PROVINCE_CODE_NAME_MAPPING.put("370000", "山东省");
		PROVINCE_CODE_NAME_MAPPING.put("410000", "河南省");
		PROVINCE_CODE_NAME_MAPPING.put("420000", "湖北省");
		PROVINCE_CODE_NAME_MAPPING.put("430000", "湖南省");
		PROVINCE_CODE_NAME_MAPPING.put("440000", "广东省");
		PROVINCE_CODE_NAME_MAPPING.put("450000", "广西壮族自治区");
		PROVINCE_CODE_NAME_MAPPING.put("460000", "海南省");
		PROVINCE_CODE_NAME_MAPPING.put("510000", "四川省");
		PROVINCE_CODE_NAME_MAPPING.put("520000", "贵州省");
		PROVINCE_CODE_NAME_MAPPING.put("530000", "云南省");
		PROVINCE_CODE_NAME_MAPPING.put("540000", "西藏自治区");
		PROVINCE_CODE_NAME_MAPPING.put("610000", "陕西省");
		PROVINCE_CODE_NAME_MAPPING.put("620000", "甘肃省");
		PROVINCE_CODE_NAME_MAPPING.put("630000", "青海省");
		PROVINCE_CODE_NAME_MAPPING.put("640000", "宁夏回族自治区");
		PROVINCE_CODE_NAME_MAPPING.put("650000", "新疆维吾尔自治区");
		PROVINCE_CODE_NAME_MAPPING.put("710000", "台湾省");
		PROVINCE_CODE_NAME_MAPPING.put("810000", "香港特别行政区");
		PROVINCE_CODE_NAME_MAPPING.put("820000", "澳门特别行政区");
	}
}