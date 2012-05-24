package cn.iie.haiep.hbase.value;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Value description in HBase.
 * @author forhappy
 * Date: 2012-4-5, 6:23 PM.
 */

public class HValue {
	public enum Type {STRING, BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE}
	
	public static class Value<T extends Object> {
		private  Logger logger = 
			LoggerFactory.getLogger(Value.class);
		
		Object object = null;
		Type type = null;
		
		public Value(T object) {
			super();
			if (object instanceof String) {
				this.object = object;
				type = Type.STRING;
			} else if (object instanceof Boolean) {
				this.object = object;
				type = Type.BOOLEAN;
			} else if (object instanceof Integer) {
				this.object = object;
				type = Type.INTEGER;
			} else if (object instanceof Long) {
				this.object = object;
				type = Type.LONG;
			} else if (object instanceof Float) {
				this.object = object;
				type = Type.FLOAT;
			} else if (object instanceof Double) {
				this.object = object;
				type = Type.DOUBLE;
			}
		}

		public byte[] getBytes() {
			switch(type) {
			case STRING:
				return Bytes.toBytes((String)object);
			case BOOLEAN:
				return Bytes.toBytes((Boolean)object);
			case INTEGER:
				return Bytes.toBytes((Integer)object);
			case LONG:
				return Bytes.toBytes((Long)object);
			case DOUBLE:
				return Bytes.toBytes((Double)object);
			default:
				logger.error("Type not supported.");
				return null;
			}
		}
	}
	
	public HValue(List<HColumn> listColumn) {
		super();
		this.listColumn = listColumn;
	}

	/**
	 * @return the listColumn
	 */
	public List<HColumn> getListColumn() {
		return listColumn;
	}

	/**
	 * @param listColumn the listColumn to set
	 */
	public void setListColumn(List<HColumn> listColumn) {
		this.listColumn = listColumn;
	}
	
	public void addColumn(HColumn column) {
		listColumn.add(column);
	}
	
	public void addColumns(List<HColumn> columns) {
		listColumn.addAll(columns);
	}
	
	public void addColumn(String family, String qualifier, Value value) {
		byte[] familyBytes = family.getBytes();
		byte[] qualifierBytes = null;
		if (qualifier != null) qualifierBytes = qualifier.getBytes();
		byte[] valueBytes = value.getBytes();
		HColumn column = new HColumn(familyBytes, qualifierBytes, valueBytes);
		listColumn.add(column);
	}
	
	public void deleteColumn(HColumn column) {
		listColumn.remove(column);
	}
	
	public Boolean hasNext(){
		if (current < listColumn.size() - 1) {
			current ++;
			return true;
		} else {
			return false;
		}
	}
	
	public HColumn next() {
		return listColumn.get(current);
	}

	private List<HColumn> listColumn = new ArrayList<HColumn>();
	
	private int current = -1;
	
	/**
	 * logger.
	 */
	private  Logger logger = 
		LoggerFactory.getLogger(HValue.class);
}
