package se.redfield.node.port.orientdb.connection;

import java.io.File;
import java.io.IOException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

import se.redfield.node.port.orientdb.util.CredentionalUtil;

public class OrientDBConnectionNodeModel extends NodeModel {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBConnectionNodeModel.class);
	private OrientDBConnectionSettings m_settings = new OrientDBConnectionSettings();

	public static final String CFGKEY_POOL_SIZE = "Connection pool size";
	public static final String CFGKEY_DB_URL = "Database URL";
	public static final String CFGKEY_REMOTE_DATABASE_NAME = "Remote database name";
	public static final String CFGKEY_USER_NAME = "Username";
	public static final String CFGKEY_PASSWORD = "Password";
	public static final String CFGKEY_CREDENTIONAL_NAME = "Credential name";

	public static final int DEFAULT_POOL_SIZE = 10;
	public static final int MIN_POOL_SIZE = 1;
	public static final int MAX_POOL_SIZE = 100;
	// public static final String DB_PATH = "embedded:./databases/";
	public static final String DEFAULT_DB_URL = "remote:localhost:2424";
	public static final String DEFAULT_DB_NAME = "demodb";
	public static final String DEFAULT_USERNAME = "admin";
	public static final String DEFAULT_PASSWORD = "admin";
	public static final String DEFAULT_CREDENTIONAL_NAME = "use login/password";

	private final SettingsModelIntegerBounded m_poolsize = new SettingsModelIntegerBounded(CFGKEY_POOL_SIZE,
			DEFAULT_POOL_SIZE, MIN_POOL_SIZE, MAX_POOL_SIZE);
	private final SettingsModelString m_db_url = new SettingsModelString(CFGKEY_DB_URL, DEFAULT_DB_URL);
	private final SettingsModelString m_remote_database_name = new SettingsModelString(CFGKEY_REMOTE_DATABASE_NAME,
			DEFAULT_DB_NAME);
	private final SettingsModelString m_user_name = new SettingsModelString(CFGKEY_USER_NAME, null);
	private final SettingsModelString m_password = new SettingsModelString(CFGKEY_PASSWORD, null);
	private final SettingsModelString m_credential_name = new SettingsModelString(CFGKEY_CREDENTIONAL_NAME, DEFAULT_CREDENTIONAL_NAME);

	protected OrientDBConnectionNodeModel() {
		super(new PortType[0], new PortType[] { OrientDBConnectionPortObject.TYPE });
	}

	@Override
	protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
		logger.info("m_db_url " + m_db_url.getStringValue());
		logger.info("m_credential_name " + m_credential_name.getStringValue());
		OrientDBConnectionSettings connectionSettings = new OrientDBConnectionSettings();
		connectionSettings.setDbUrl(m_db_url.getStringValue());
		connectionSettings.setPoolSize(m_poolsize.getIntValue());
		connectionSettings.setDbName(m_remote_database_name.getStringValue());
		connectionSettings.setUserName(m_user_name.getStringValue());
		connectionSettings.setPassword(m_password.getStringValue());
		connectionSettings.setCredName(m_credential_name.getStringValue());
		// check connection to OrientDB server
		checkConnection();
		OrientDBConnectionPortObject dbPort = new OrientDBConnectionPortObject(
				new OrientDBConnectionPortObjectSpec(connectionSettings));
		return new PortObject[] { dbPort };
	}

	private void checkConnection() throws InvalidSettingsException {
		logger.infoWithFormat("Checking connection to server %s , database %s ", m_db_url.getStringValue(),
				m_remote_database_name.getStringValue());
		
		logger.infoWithFormat("cred name: %s", m_credential_name.getStringValue());
		
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(m_user_name.getStringValue(), m_password.getStringValue(),
				m_credential_name.getStringValue(), getCredentialsProvider());
		
		OrientDB orientDBEnv = new OrientDB(m_db_url.getStringValue(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv, m_remote_database_name.getStringValue(),
				userLogin.getLogin(),
				userLogin.getDecryptedPassword());
		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			logger.infoWithFormat("database status : %s", databaseSession.getStatus());
		} catch (Exception e) {
			throw new InvalidSettingsException("Can't connect to database. Check parameters or database availability !",
					e);
		} finally {
			orientDBPool.close();
			orientDBEnv.close();
		}
	}

	@Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		return new PortObjectSpec[] { new OrientDBConnectionPortObjectSpec(new OrientDBConnectionSettings()) };
	}

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
	}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		logger.info("===saveSettingsTo====");
		m_poolsize.saveSettingsTo(settings);
		m_remote_database_name.saveSettingsTo(settings);
		m_db_url.saveSettingsTo(settings);
		m_user_name.saveSettingsTo(settings);
		m_password.saveSettingsTo(settings);
		m_credential_name.saveSettingsTo(settings);


		m_settings.setPoolSize(m_poolsize.getIntValue());
		m_settings.setDbUrl(m_db_url.getStringValue());
		m_settings.setDbName(m_remote_database_name.getStringValue());
		m_settings.setUserName(m_user_name.getStringValue());
		m_settings.setPassword(m_password.getStringValue());

		m_settings.saveConnection(settings);

	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		logger.infoWithFormat("==validateSettings==. %s", settings);
		m_poolsize.validateSettings(settings);
		m_remote_database_name.validateSettings(settings);
		m_db_url.validateSettings(settings);
		m_user_name.validateSettings(settings);
		m_password.validateSettings(settings);
		m_credential_name.validateSettings(settings);
	}

	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_poolsize.loadSettingsFrom(settings);
		m_remote_database_name.loadSettingsFrom(settings);
		m_db_url.loadSettingsFrom(settings);
		m_user_name.loadSettingsFrom(settings);
		m_password.loadSettingsFrom(settings);
		m_credential_name.loadSettingsFrom(settings);
	}

	@Override
	protected void reset() {
		// TODO Auto-generated method stub

	}

}
