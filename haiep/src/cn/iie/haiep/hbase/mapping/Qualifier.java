package cn.iie.haiep.hbase.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author forhappy
 * Date: 2012-4-3, 9:44 PM.
 */

public class Qualifier {
	
	public Qualifier() {}
	/**
	 * @param name The name of a qualifier.
	 * @param field The original column from which in RDBMS exports 
	 * this column in HBase. 
	 * @param type The original data type of a column in RDBMS. 
	 * @param condition Only the column in RDBMS that satisfied with the specifying condition
	 * can be exported.
	 */
	public Qualifier(String name, String field, String type,
			String condition) {
		super();
		this.name = name;
		this.field = field;
		this.type = type;
		this.condition = condition;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the origColumn
	 */
	public String getField() {
		return field;
	}

	/**
	 * @param field the origColumn to set
	 */
	public void setField(String field) {
		this.field = field;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the condition
	 */
	public String getCondition() {
		return condition;
	}

	/**
	 * @param condition the condition to set
	 */
	public void setCondition(String condition) {
		this.condition = condition;
	}

	/**
	 * The name of a qualifier.
	 */
	private String name = null;
	
	/**
	 * The original column from which in RDBMS exports 
	 * this column in Hbase. 
	 */
	private String field = null;
	
	/**
	 * The original data type of a column in RDBMS. 
	 */
	private String type = null;
	
	/**
	 * Only the column in RDBMS that satisfied with the condition
	 * can be exported, if it remains null, no condition is restricted.
	 */
	private String condition = null;

	/**
	 * logger.
	 */
	public static final Logger logger = LoggerFactory.getLogger(Qualifier.class);
}
