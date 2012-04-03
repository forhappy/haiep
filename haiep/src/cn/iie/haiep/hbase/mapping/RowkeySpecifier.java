package cn.iie.haiep.hbase.mapping;
/**
 * @author forhappy
 * Date: 2012-4-3, 10:18 PM.
 */
import java.util.ArrayList;
import java.util.List;

public class RowkeySpecifier {
	
	/**
	 * @return the index
	 */
	public int getIndex() {
		return index;
	}

	/**
	 * @param index the index to set
	 */
	public void setIndex(int index) {
		this.index = index;
	}

	/**
	 * @return the columns
	 */
	public List<String> getColumns() {
		return columns;
	}

	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public RowkeySpecifier(){
		index = -1;
		columns = new ArrayList<String>();
	}
	
	public RowkeySpecifier(List<String> columns) {
		index = -1;
		this.columns = columns;
	}
	
	public void addColumnToRowkey(String e) {
		columns.add(e);
	}
	
	/**
	 * Check if there exists another column name that is part of a rowkey
	 * in the List<String> columns.
	 * @return True if it indeed exist a column name, 
	 * otherwise return false.
	 */
	public Boolean next() {
		if (columns.isEmpty()) return false;
		if (index < columns.size() - 1) {
			index++;
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Return the current column name pointed by index,
	 * if List<String> columns is empty, return null.
	 * @return
	 */
	public String current() {
		if (columns.isEmpty()) return null;
		return columns.get(index);
	}
	
	private int index = -1;
	private List<String> columns = null;
}
