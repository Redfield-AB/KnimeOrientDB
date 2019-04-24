package se.redfield.node.port.orientdb.util;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class OrientDbUtil {

	public static int getMaxPoolSize() {
		return  OrientDBConfig.defaultConfig().getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX);
	}
	
}
