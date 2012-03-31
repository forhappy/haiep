package cn.iie.haiep.configuration;
/**
 * Configuration reader.
 * @author forhappy
 * Data: 2012-3-31 8:35 PM.
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Configuration {
	
	public static final Logger logger = LoggerFactory.getLogger(Configuration.class);
	
	public static final String CONFIGURATION_DIR = "conf";
	
	public static final String HAIEP_DEFAULT_PROPERTIES_FILE = "haiep.properties";

	public static final String HAIEP = "haiep";
	
	public static final String MAPPING_FILE = "hbase-mapping.xml";
	
	private Properties property = null;
	
	private String confFilePath = CONFIGURATION_DIR + "/" + HAIEP_DEFAULT_PROPERTIES_FILE;
	
	private FileInputStream confFile = null;
	
	/**
	 * Default constructor.
	 */
	public Configuration() {
		property = new Properties();
		try {
			confFile = new FileInputStream(new File(confFilePath));
			try {
				property.load(confFile);
				confFile.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor using configuration file path and load property.
	 * @param confFilePath
	 */
	public Configuration(String confFilePath) {
		property = new Properties();
        try {
            confFile = new FileInputStream(new File(confFilePath));
            property.load(confFile);
            confFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	

	/**
	 * Get value specified by key
	 * @param key specify the property name.
	 * @return value specified by key iff there exits the property, 
	 * otherwise return nothing.
	 */
	public String getValue(String key)
    {
        if(property.containsKey(key)){
            String value = property.getProperty(key);
            return value;
        }
        else 
            return "";
    }
	
	/**
	 * Get value specified by key in fileName.
	 * @param fileName file name.
	 * @param key specify the property name.
	 * @return value specified by key iff there exits the property,
	 * otherwise return nothing.
	 */
	public String getValue(String fileName, String key) {
		try {
			String value = "";
			FileInputStream tmpConfFile = new FileInputStream(new File(fileName));
			property.load(tmpConfFile);
			tmpConfFile.close();
			if (property.containsKey(key)) {
				value = property.getProperty(key);
				return value;
			} else
				return value;
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return "";
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return "";
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return "";
		}
	}
	
	/**
	 * clear properties.
	 */
	public void clear()
    {
        property.clear();
    }
}
