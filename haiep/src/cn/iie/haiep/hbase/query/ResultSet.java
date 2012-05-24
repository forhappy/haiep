package cn.iie.haiep.hbase.query;

import java.util.ArrayList;
import java.util.List;

public class ResultSet {
	private List<List<String>> v = new ArrayList<List<String>>();
	private int current = -1;
	
	public ResultSet() {
	}
	
	public String getString(int indexColumn) {
		return v.get(current).get(indexColumn);
	}
	
	public void addResultRow(ArrayList<String> vec) {
		v.add(vec);
	}
	
	public Boolean next(){
		if (current < v.size() - 1) {
			current ++;
			return true;
		} else {
			return false;
		}
	}
}
