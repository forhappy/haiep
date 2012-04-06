package cn.iie.haiep.hbase.key;

import cn.iie.haiep.hbase.value.HValue.Value;

/**
 * Key description in HBase.
 * @author forhappy
 * Date: 2012-4-3, 6:22 PM.
 */

public class HKey {
	public HKey(Value value) {
		row = value.getBytes();
	}
	
	public HKey(byte[] row) {
		super();
		this.row = row;
	}

	/**
	 * @return the row
	 */
	public byte[] getRow() {
		return row;
	}

	/**
	 * @param row the row to set
	 */
	public void setRow(byte[] row) {
		this.row = row;
	}

	private byte[] row = null;
}
