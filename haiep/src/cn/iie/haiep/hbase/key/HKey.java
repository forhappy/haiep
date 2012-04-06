package cn.iie.haiep.hbase.key;

import java.util.Arrays;

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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(row);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HKey other = (HKey) obj;
		if (!Arrays.equals(row, other.row))
			return false;
		return true;
	}
}
