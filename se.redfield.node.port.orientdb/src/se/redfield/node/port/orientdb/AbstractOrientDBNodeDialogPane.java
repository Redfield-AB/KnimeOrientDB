package se.redfield.node.port.orientdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.DialogComponent;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;

import se.redfield.node.port.orientdb.connection.OrientDBConnectionSettings;
import se.redfield.node.port.orientdb.util.CredentionalUtil;

public abstract class AbstractOrientDBNodeDialogPane extends NodeDialogPane  {
	private static final NodeLogger logger = NodeLogger.getLogger(AbstractOrientDBNodeDialogPane.class);
	private final List<DialogComponent> dialogComponents = new ArrayList<DialogComponent>();
	
	
	protected Collection<String> getAvailableClasses(Predicate<OClass> classFilter) throws InvalidSettingsException {
		logger.info("=!== getAvailableClasses ====");
		if (getConnectionSettings() == null) {
			logger.warn("no connection info. Go out!");
			return Collections.singletonList("no classes!");
		}
		logger.info("=!== getDbName :" + getConnectionSettings().getDbName());
		List<String> availableClasses = new LinkedList<String>();
		
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(getConnectionSettings().getUserName(), getConnectionSettings().getPassword(),
				getConnectionSettings().getCredName(), getCredentialsProvider());
		OrientDB orientDBEnv = new OrientDB(getConnectionSettings().getDbUrl(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv, getConnectionSettings().getDbName(),
				userLogin.getLogin(), userLogin.getDecryptedPassword());

		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			OMetadata metadata = databaseSession.getMetadata();
			OSchema schema = metadata.getSchema();

			availableClasses = schema.getClasses().stream().filter(classFilter).map((OClass c) -> {
				return c.getName();
			}).sorted().collect(Collectors.toList());
			logger.info("=!== availableClasses:"+availableClasses);
		} finally {
			orientDBPool.close();
			orientDBEnv.close();
		}

		return availableClasses;
	}
	
	protected Map<String,List<OProperty>> getAvailableClassesWithFields(Predicate<OClass> classFilter) throws InvalidSettingsException {
		logger.info("=!== getAvailableClassesWithFields ====");
		if (getConnectionSettings() == null) {
			logger.warn("no connection info. Go out!");
			return Collections.emptyMap();
		}
		logger.info("=!== getDbName :" + getConnectionSettings().getDbName());
		List<OClass> availableClasses = new LinkedList<>();
		Map<String,List<OProperty>> resultMap = new LinkedHashMap<String, List<OProperty>>();
		
		
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(getConnectionSettings().getUserName(), getConnectionSettings().getPassword(),
				getConnectionSettings().getCredName(), getCredentialsProvider());
		
		
		OrientDB orientDBEnv = new OrientDB(getConnectionSettings().getDbUrl(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv, getConnectionSettings().getDbName(),
				userLogin.getLogin(), userLogin.getDecryptedPassword());

		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			OMetadata metadata = databaseSession.getMetadata();
			OSchema schema = metadata.getSchema();
			availableClasses = schema.getClasses().stream().filter(classFilter)
					.sorted((OClass c1,OClass c2)->c1.getName().compareTo(c2.getName()))
						.collect(Collectors.toList());
			
			for  (OClass schemaClass : availableClasses) {
				resultMap.put(schemaClass.getName(), new LinkedList<>(schemaClass.properties()));
			}
			
			
			logger.info("=!== availableClasses:"+availableClasses);
		} finally {
			orientDBPool.close();
			orientDBEnv.close();
		}

		return resultMap;
	}
	
	
	
	protected List<DialogComponent> getDialogComponents() {
		return dialogComponents;
	}


	abstract protected OrientDBConnectionSettings getConnectionSettings();

}
