package cn.iie.haiep.hbase.query;

public class QueryStateInfoTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String date = "20120510";
		
		String resultQueryStateInfoByDay = QueryStateInfo.queryStateInfoByDay(date);
		System.out.println(resultQueryStateInfoByDay);
		
//		date = "201205";
//		String resultQueryStateInfoByMonth = QueryStateInfo.queryStateInfoByMonth(date);
//		System.out.println(resultQueryStateInfoByMonth);
		
		date = "2012";
		String resultQueryStateInfoByQuarter = QueryStateInfo.queryStateInfoByQuater(date);
		System.out.println(resultQueryStateInfoByQuarter);
		
//		date = "2012";
//		String resultQueryStateInfoByYear = QueryStateInfo.queryStateInfoByYear(date);
//		System.out.println(resultQueryStateInfoByYear);

	}

}
