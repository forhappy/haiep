package cn.iie.haiep.hbase.value;

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
	
}
