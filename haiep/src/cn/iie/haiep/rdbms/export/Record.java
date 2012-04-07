/**
 * forhappy
 * Date: 6:58:00 PM, Apr 7, 2012
 */
package cn.iie.haiep.rdbms.export;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Record {
	
	/**
	 * @return the record
	 */
	public Map<String, Object> getRecord() {
		return record;
	}

	/**
	 * @param record the record to set
	 */
	public void setRecord(Map<String, Object> record) {
		this.record = record;
		iterRecord = this.record.entrySet().iterator();
	}

	public Record(Map<String, Object> record) {
		super();
		this.record = record;
		iterRecord = this.record.entrySet().iterator();
	}
	
	public void addKeyValue(String key, Object value) {
		record.put(key, value);
	}
	
	public void put(String key, Object value) {
		record.put(key, value);
	}

	public Boolean hasNext() {
		return iterRecord.hasNext();
	}
	
	public Entry next() {
		Entry thisEntry = (Entry)iterRecord.next();
		return thisEntry;
	}
	
	Map<String, Object> record = 
		new HashMap<String, Object>();
	
	/**
	 * iterator of record.
	 */
	private Iterator iterRecord = null;
}
