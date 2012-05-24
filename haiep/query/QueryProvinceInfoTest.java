package cn.iie.haiep.hbase.query;

public class QueryProvinceInfoTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String date = "20120510";
		
		String resultQueryProvinceInfoByDay = QueryProvinceInfo.queryProvinceInfoByDay(date);
		System.out.println(resultQueryProvinceInfoByDay);
		
//		date = "201205";
//		String resultQueryProvinceInfoByMonth = QueryProvinceInfo.queryProvinceInfoByMonth(date);
//		System.out.println(resultQueryProvinceInfoByMonth);
		
		date = "2012";
		String resultQueryProvinceInfoByQuarter = QueryProvinceInfo.queryProvinceInfoByQuater(date, "130000");
		System.out.println(resultQueryProvinceInfoByQuarter);
		
//		date = "2012";
//		String resultQueryProvinceInfoByYear = QueryProvinceInfo.queryProvinceInfoByYear(date);
//		System.out.println(resultQueryProvinceInfoByYear);
	}

}
