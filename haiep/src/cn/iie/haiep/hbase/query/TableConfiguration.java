package cn.iie.haiep.hbase.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class TableConfiguration {
	static private HTable table;
	static {
		Configuration conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, Constants.CONFIG_TABLE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static public String get(byte[] key, String defaultValue) {
		Get get = new Get(key);

		try {
			Result result = table.get(get);

			if (result.isEmpty()) {
				return defaultValue;
			}

			byte[] vbytes = result.getValue(Constants.CONFIG_VALUE[0],
					Constants.CONFIG_VALUE[1]);

			if (vbytes == null) {
				return defaultValue;
			}

			return new String(vbytes);
		} catch (IOException e) {
			e.printStackTrace();
			return defaultValue;
		}
	}

	static public void set(byte[] key, String value) {
		Put put = new Put(key);

		put.add(Constants.CONFIG_VALUE[0], Constants.CONFIG_VALUE[1], 0,
				value.getBytes());

		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		TableConfiguration.set(Constants.CONFIG_LAST_IMPORT_TIME, "2012-05-06 12:23:32");
	}
}
