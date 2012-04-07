/**
 * forhappy
 * Date: 5:58:00 PM, Apr 7, 2012
 */
package cn.iie.haiep.rdbms.export;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataTable {
	
	
	public DataTable(List<Record> dataTable) {
		super();
		this.dataTable = dataTable;
		iterDataTable = this.dataTable.iterator();
	}

	/**
	 * @return the dataTable
	 */
	public List<Record> getDataTable() {
		return dataTable;
	}

	/**
	 * @param dataTable the dataTable to set
	 */
	public void setDataTable(List<Record> dataTable) {
		this.dataTable = dataTable;
		iterDataTable = this.dataTable.iterator();
	}
	
	public void addRecord(Record record) {
		dataTable.add(record);
	}

	public Boolean hasNext() {
		return iterDataTable.hasNext();
	}
	
	public Record next() {
		return (Record) iterDataTable.next();
	}
	
	/**
	 * data table in database, which contains all rows in the database.
	 */
	private List<Record> dataTable = 
		new ArrayList<Record>();
	
	/**
	 * iterator of data table.
	 */
	private Iterator iterDataTable = null;
}
