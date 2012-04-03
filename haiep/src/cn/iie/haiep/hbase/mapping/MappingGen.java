package cn.iie.haiep.hbase.mapping;
/**
 * @author forhappy
 * Date: 2012-4-3, 7:38 PM.
 */
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Illustrate the mapping between RDBMS and HBase based on the configuration file.
 * here by is a example configuration that specifies the mapping.
 * <?xml version="1.0" encoding="UTF-8"?>
 * <mapping source="eid_verification_s" target="eid_verification_t">
 *   <family name="id">           <!-- A family in hbase-->
 *     <qualifier>                <!-- A qualifier within the family-->
 *       <name>id_type</name>     <!-- Qualifier name-->
 *       <field>id_type</field> <!-- Original column name in RDMS-->
 *       <type>STRING</type>      <!-- Original column type in RDMS-->
 *       <condition></condition>  <!-- Query condition-->
 *     </qualifier>
 *     <qualifier>                <!-- Another qualifier within the family-->
 *       <name>id</name>
 *       <field>id</field>
 *       <type>STRING</type>
 *       <condition></condition>
 *     </qualifier>
 *     <qualifier>                <!-- The other qualifier within the family-->
 *       <name>id_2nd</name>
 *       <field>id_2nd</field>
 *       <type>BOOLEAN</type>
 *       <condition></condition>
 *     </qualifier>
 *   </family>
 *   
 *   <family name="username">
 *     <qualifier>
 *       <name></name>
 *       <field>username</field>
 *       <type>STRING</type>
 *       <condition></condition>
 *     </qualifier>
 *   </family>
 *   
 *   <family name="netcafe_address">
 *     <qualifier>
 *       <name></name>
 *       <field>netcafe_address</field>
 *       <type>STRING</type>
 *       <condition></condition>
 *     </qualifier>
 *   </family>
 * </mapping> 
 * 
 * @author forhappy
 * 
 */
public class MappingGen {

	/**
	 * @return the rowkey
	 */
	public RowkeySpecifier getRowkey() {
		return rowkey;
	}

	/**
	 * @param rowkey the rowkey to set
	 */
	public void setRowkey(RowkeySpecifier rowkey) {
		this.rowkey = rowkey;
	}

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
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	/**
	 * @return the source
	 */
	public String getSource() {
		return source;
	}

	/**
	 * @return the target
	 */
	public String getTarget() {
		return target;
	}

	/**
	 * Default constructor.
	 */
	public MappingGen() {
		index = -1;
		families = new ArrayList<Family>();
	}

	/**
	 * Custom constructor.
	 * 
	 * @param filePath
	 *            Specifies the file path of mapping schema.
	 */
	public MappingGen(String filePath) {
		this.filePath = filePath;
		index = -1;
		families = new ArrayList<Family>();
	}

	/**
	 * Parse the mapping schema specified the configuration file.
	 */
	public void parseMapping() {
		try {
			File xmlFile = new File(filePath);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
			Document doc = documentBuilder.parse(xmlFile);
			doc.getDocumentElement().normalize();
			source = doc.getDocumentElement().getAttribute("source");
			target = doc.getDocumentElement().getAttribute("target");
			
			RowkeySpecifier rowkey = new RowkeySpecifier();
			NodeList nodeListOfRowkeys = doc.getElementsByTagName("rowkey");
			
			for (int j = 0; j < nodeListOfRowkeys.getLength(); j++) {
				Node nodeRowkey = nodeListOfRowkeys.item(j);
				
				if (nodeRowkey.getNodeType() == Node.ELEMENT_NODE) {
					Element elementRowkey = (Element) nodeRowkey;
					NodeList columnNodeList = elementRowkey.getChildNodes();
					for (int t = 0; t < columnNodeList.getLength(); t++) {
						String column  = null;
						Node columnNode = columnNodeList.item(t);
						if (columnNode.getNodeType() == Node.ELEMENT_NODE) {
							Element columnElement = (Element) columnNode;
							NodeList nodeList = columnElement.getChildNodes();
							if (nodeList.getLength() != 0) {
								Node nodeValue = (Node) nodeList.item(0);
								column = nodeValue.getNodeValue();
								rowkey.addColumnToRowkey(column);
							}
						}
					}
				}
			}
			
			setRowkey(rowkey);
			
			NodeList nodeListOfFamilies = doc.getElementsByTagName("family");
			
			
			for (int i = 0; i < nodeListOfFamilies.getLength(); i++) {
				Node nodeFamily = nodeListOfFamilies.item(i);
				NamedNodeMap familyAttributes = nodeFamily.getAttributes();
				String familyAttribute = familyAttributes.getNamedItem("name").getNodeValue();
				
				Family family = new Family(familyAttribute);
				
				if (nodeFamily.getNodeType() == Node.ELEMENT_NODE) {
					Element elementFamily = (Element) nodeFamily;
					NodeList qualifierNodeList = elementFamily.getChildNodes();
					for (int t = 0; t < qualifierNodeList.getLength(); t++) {
						Qualifier qualifier = new Qualifier();
						Node qualifierNode = qualifierNodeList.item(t);
						if (qualifierNode.getNodeType() == Node.ELEMENT_NODE) {
							Element qualifierElement = (Element) qualifierNode;
							qualifier.setName(getQualiferBySubElementValue("name", qualifierElement));
							qualifier.setField(getQualiferBySubElementValue("field", qualifierElement));
							qualifier.setType(getQualiferBySubElementValue("type", qualifierElement));
							qualifier.setCondition(getQualiferBySubElementValue("condition", qualifierElement));
							family.addQualifier(qualifier);
						}
					}
				}
				families.add(family);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get a list of qualifiers by family name.
	 * @param familyName Family name.
	 * @return A list of qualifiers within the specified family.
	 */
	public List<Qualifier> getQualifiersByFamilyName(String familyName) {
		if (families.isEmpty()) return null;
		List<Qualifier> list = new ArrayList<Qualifier>();
		for (int i = 0; i < families.size(); i++) {
			Family family = families.get(i);
			if (family.getName().equalsIgnoreCase(familyName)) {
				list = family.getQualifiers();
			}
		}
		return list;
	}
	
	/**
	 * Get a Family object by the family name.
	 * @param familyName Family name.
	 * @return  A Family object.
	 */
	public Family getFamilyByName(String familyName) {
		if (families.isEmpty()) return null;
		Family family = new Family();
		for (int i = 0; i < families.size(); i++) {
			if (families.get(i).getName().equalsIgnoreCase(familyName)) {
				family = families.get(i);
			}
		}
		return family;
	}
	
	/**
	 * Check if there exists another Family object 
	 * in the List<Family> families.
	 * @return True if it indeed exist a Family object, 
	 * otherwise return false.
	 */
	public Boolean next() {
		if (families.isEmpty()) return false;
		if (index < families.size() - 1) {
			index++;
			return true;
		} else {
			return false;
		}
	}
	/**
	 * Reset index to -1, it means the start of List<Family> families.
	 */
	public void reset() {
		index = -1;
	}
	
	/**
	 * Return the current Family object pointed by index,
	 * if List<Family> families is empty, return null.
	 * @return
	 */
	public Family current() {
		if (families.isEmpty()) return null;
		return families.get(index);
	}
	/**
	 * Get qualifier sub-element by tag.
	 * @param tag Sub-element tag.
	 * @param element
	 * @return
	 */
	private  String getQualiferBySubElementValue(String tag, Element element) {
		if (element.getElementsByTagName(tag).getLength() == 0) {
			return null;
		} else {
			NodeList nlList = element.getElementsByTagName(tag).item(0)
					.getChildNodes();
			if (nlList.getLength() == 0) {
				return null;
			} else {
				Node nValue = (Node) nlList.item(0);
				return nValue.getNodeValue();
			}
		}
	}

	/**
	 * File path that specifies the mapping schema.
	 */
	private String filePath = null;

	/**
	 * The source table name of RDMS from which data will export.
	 */
	private String source = null;

	/**
	 * The target table name of HBase into which data will import.
	 */
	private String target = null;

	/**
	 * Family list that illustrates the tables in HBase.
	 */
	private List<Family> families = null;
	
	private RowkeySpecifier rowkey = null;
	
	/**
	 * Index that specify the current family.
	 */
	private int index = -1;
	
	/**
	 * logger.
	 */
	public static final Logger logger = LoggerFactory.getLogger(MappingGen.class);
}