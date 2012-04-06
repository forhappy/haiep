package cn.iie.haiep.hbase.value;

import java.util.Arrays;

/**
 * Value description in HBase.
 * @author forhappy
 * Date: 2012-4-5, 9:36 PM.
 */

public class HColumn {
	/**
	 * Constructor
	 * @param family family name.
	 * @param qualifier qualifier.
	 * @param value value.
	 */
	public HColumn(byte[] family, byte[] qualifier, byte[] value) {
		super();
		this.family = family;
		this.qualifier = qualifier;
		this.value = value;
	}

	/**
	 * family name.
	 */
	private byte[] family = null; 
	
	/**
	 * qualifier.
	 */
	private byte[] qualifier = null;
	
	/**
	 * value
	 */
	private byte[] value = null;

	/**
	 * @return the family
	 */
	public byte[] getFamily() {
		return family;
	}

	/**
	 * @param family the family to set
	 */
	public void setFamily(byte[] family) {
		this.family = family;
	}

	/**
	 * @return the qualifier
	 */
	public byte[] getQualifier() {
		return qualifier;
	}

	/**
	 * @param qualifier the qualifier to set
	 */
	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}

	/**
	 * @return the value
	 */
	public byte[] getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(byte[] value) {
		this.value = value;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(family);
		result = prime * result + Arrays.hashCode(qualifier);
		result = prime * result + Arrays.hashCode(value);
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
		HColumn other = (HColumn) obj;
		if (!Arrays.equals(family, other.family))
			return false;
		if (!Arrays.equals(qualifier, other.qualifier))
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}
	
}
