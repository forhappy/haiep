package cn.iie.haiep.hbase.mapping;


public class MappingGeneratorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MappingGen mapping = new MappingGen();
		String filePath = "/home/forhappy/SCM-Repos/GIT/Haiep/haiep/conf/haiep-mapping-example.xml";
		mapping.setFilePath(filePath);
		mapping.parseMapping();
		System.out.println("Source Table: " + mapping.getSource());
		System.out.println("Target Table: " + mapping.getTarget());
		
/*		List<QualifierGenerator> list = mapping.getQualifiersByFamilyName("id");
		
		for (int i = 0; i < list.size(); i++) {
			QualifierGenerator qualifier = list.get(i);
			System.out.println(qualifier.getName());
			System.out.println(qualifier.getOrigColumn());
			System.out.println(qualifier.getType());
			System.out.println(qualifier.getCondition());
		}*/
		
/*		FamilyGenerator family = mapping.getFamilyByName("id");
		System.out.println(family.getName());
		List<QualifierGenerator> list1 = family.getQualifiers();
		for (int i = 0; i < list.size(); i++) {
			QualifierGenerator qualifier = list1.get(i);
			System.out.println(qualifier.getName());
			System.out.println(qualifier.getOrigColumn());
			System.out.println(qualifier.getType());
			System.out.println(qualifier.getCondition());
		}*/
		Family family;
		while (mapping.next()) {
			family = mapping.current();
			
			System.out.println("Family Name: " + family.getName());
			/**
			 * test getQualifierByField
			 */
			String field = "id_2nd";
			Qualifier qualifier = family.getQualifierByName(field);
			if (qualifier != null) {
				System.out.println("===================");
				System.out.println("Qualifier Name: " + qualifier.getName());
				System.out.println("Qualifier Field: " + qualifier.getField());
				System.out.println("Qualifier Type: " + qualifier.getType());
				System.out.println("Qualifier Condition: " + qualifier.getCondition());
				System.out.println("===================");
			}
			
/*			while (family.next()) {
				Qualifier qualifier = family.current();
				System.out.println("===================");
				System.out.println("Qualifier Name: " + qualifier.getName());
				System.out.println("Qualifier Field: " + qualifier.getField());
				System.out.println("Qualifier Type: " + qualifier.getType());
				System.out.println("Qualifier Condition: " + qualifier.getCondition());
				System.out.println("===================");
				System.out.println("");
			}*/
			System.out.println("");
			System.out.println("");
		}
		
		RowkeySpecifier rowkey = mapping.getRowkey();
		
		while (rowkey.next()) {
			System.out.println(rowkey.current());
		}
	}
}