package daogenerator.utils.connectionPool;

import java.io.InputStream;
import java.util.Properties;

public class ReadDBProperties {
	private final Properties properties;
	private final String webUrl = "";

	public ReadDBProperties() {
		properties = new Properties();
		InputStream in = null;
		try {
			ClassLoader loader = ReadDBProperties.class.getClassLoader(); 
			in = loader.getResourceAsStream("daogenerator_config.properties");
			properties.load(in); 
			in.close(); 
			in = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Properties getProperties() {
		return properties;
	}

	public String getWebUrl() {
		return webUrl;
	}
}