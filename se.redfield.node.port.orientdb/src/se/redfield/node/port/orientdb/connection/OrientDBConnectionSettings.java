package se.redfield.node.port.orientdb.connection;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.util.KnimeEncryption;
import org.knime.core.util.Version;

import se.redfield.node.port.orientdb.OrientDBConnectionKeys;
import se.redfield.node.port.orientdb.query.OrientDBQueryNodeModel;

public class OrientDBConnectionSettings {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(OrientDBConnectionSettings.class);

	static final String KEY_ORIENTDB_CONNECTION = "orientdb_connection.zip";

	public static final String CFGKEY_CREDENTIAL_NAME = "credential_name";

	private String credName;

	private String dbUrl;
	private String dbName;
	private String userName = null;
	private String password = null;
	private int poolSize = OrientDBConnectionNodeModel.DEFAULT_POOL_SIZE;

	private boolean m_kerberos = false;

	public OrientDBConnectionSettings(OrientDBConnectionSettings conn) {
		LOGGER.info("=1===OrientDBConnectionSettings====");
		this.poolSize = conn.getPoolSize();
		this.dbUrl = conn.getDbUrl();
		this.dbName = conn.getDbName();
		this.dbUrl = conn.getDbUrl();
		this.password = conn.getPassword();
		this.credName = conn.getCredName();

	}

	public OrientDBConnectionSettings(final ConfigRO settings, final CredentialsProvider credProvider)
			throws InvalidSettingsException {
		LOGGER.info("=2===OrientDBConnectionSettings====");
		loadValidatedConnection(settings, credProvider);
	}

	public OrientDBConnectionSettings() {
		// TODO Auto-generated constructor stub
	}

	public boolean loadValidatedConnection(final ConfigRO settings, final CredentialsProvider cp)
			throws InvalidSettingsException {
		return loadConnection(settings, true, cp);
	}

	private boolean loadConnection(final ConfigRO settings, final boolean write, final CredentialsProvider cp)
			throws InvalidSettingsException {
		if (settings == null) {
			throw new InvalidSettingsException("Connection settings not available!");
		}
		LOGGER.info("loadConnection. settings.keySet() :" + settings.keySet());
		LOGGER.info("loadConnection. userName :" + settings.getString(OrientDBConnectionKeys.CFGKEY_USER_NAME));
		LOGGER.info("loadConnection. credName :" + settings.getString(OrientDBConnectionKeys.CFGKEY_CREDENTIAL_NAME));
		
		setPoolSize(settings.getInt(OrientDBConnectionKeys.CFGKEY_POOL_SIZE,OrientDBConnectionNodeModel.DEFAULT_POOL_SIZE));
		setDbName(settings.getString(OrientDBConnectionKeys.CFGKEY_REMOTE_DATABASE_NAME,OrientDBConnectionNodeModel.DEFAULT_DB_NAME));
		setDbUrl(settings.getString(OrientDBConnectionKeys.CFGKEY_DB_URL,OrientDBConnectionNodeModel.DEFAULT_DB_URL));
		setCredName(settings.getString(OrientDBConnectionKeys.CFGKEY_CREDENTIAL_NAME,OrientDBConnectionNodeModel.DEFAULT_CREDENTIONAL_NAME));
		try {
			LOGGER.info("loadConnection. cp.listNames() :" + cp.listNames());
		} catch (NullPointerException e) {
			// TODO: handle exception
		}
		boolean useCredential = false;
		try {
			useCredential = (getCredName() != null && cp != null && cp.listNames() != null
					&& cp.listNames().contains(getCredName()));
		} catch (NullPointerException e) {
			// TODO: handle exception
		}
		LOGGER.info("loadConnection. useCredential :" + useCredential);
		if (useCredential) {			
			if (cp != null) {
				try {
					ICredentials cred = cp.get(getCredName());
					setUserName(cred.getLogin());
					setPassword(cred.getPassword());
					if (password == null) {
						LOGGER.warn("Credentials/Password has not been set, using empty password.");
					}
				} catch (IllegalArgumentException e) {
					if (!write) {
						throw new InvalidSettingsException(e.getMessage());
					}
				}
			}
		} else {
			setUserName(settings.getString(OrientDBConnectionKeys.CFGKEY_USER_NAME,OrientDBConnectionNodeModel.DEFAULT_USERNAME));
			final String pw = settings.getString(OrientDBConnectionKeys.CFGKEY_PASSWORD, OrientDBConnectionNodeModel.DEFAULT_PASSWORD);
			String password = null;
			if (pw != null) {
				try {
					password = KnimeEncryption.decrypt(pw);
				} catch (Exception e) {
					LOGGER.error("Password could not be decrypted, reason: " + e.getMessage());
				}
			} else {
				password = settings.getPassword(OrientDBConnectionKeys.CFGKEY_PASSWORD_ENCRYPTED, ";Op5~pK{31AIN^eH~Ab`:YaiKM8CM`8_Dw:1Kl4_WHrvuAXO"
						,OrientDBConnectionNodeModel.DEFAULT_PASSWORD);
			}
			setPassword(password);
		}
		
		return true;
	}
	
	public void saveConnection(final ModelContent settings) {
		LOGGER.info("=1===saveConnection===");
		LOGGER.infoWithFormat("keys : %s ",settings.keySet());
		LOGGER.infoWithFormat("getPoolSize() : %s ", getPoolSize());
		LOGGER.infoWithFormat("getDbName() : %s ", getDbName());
		LOGGER.infoWithFormat("getUserName() : %s ", getUserName());
		LOGGER.infoWithFormat("getCredName() : %s ", getCredName());
		
		settings.addString(OrientDBConnectionKeys.CFGKEY_DB_URL, getDbUrl());
		settings.addString(OrientDBConnectionKeys.CFGKEY_REMOTE_DATABASE_NAME, getDbName());
		
		settings.addString(OrientDBConnectionKeys.CFGKEY_USER_NAME, getUserName());
		settings.addString(OrientDBConnectionKeys.CFGKEY_PASSWORD, getPassword());
		settings.addString(OrientDBConnectionKeys.CFGKEY_CREDENTIAL_NAME, getCredName());
		
		settings.addInt(OrientDBConnectionKeys.CFGKEY_POOL_SIZE, getPoolSize());
		
	}

	public void saveConnection(final ConfigWO settings) {
		LOGGER.info("=2===saveConnection===");
		NodeSettings nodeSettings = (NodeSettings) settings;
		LOGGER.infoWithFormat("keys : %s ",nodeSettings.keySet());
		
		
		nodeSettings.addString(OrientDBConnectionKeys.CFGKEY_DB_URL, getDbUrl());
		nodeSettings.addString(OrientDBConnectionKeys.CFGKEY_REMOTE_DATABASE_NAME, getDbName());
		
		nodeSettings.addString(OrientDBConnectionKeys.CFGKEY_USER_NAME, getUserName());
		nodeSettings.addString(OrientDBConnectionKeys.CFGKEY_PASSWORD, getPassword());
		nodeSettings.addString(OrientDBConnectionKeys.CFGKEY_CREDENTIAL_NAME, getCredName());
		
		nodeSettings.addInt(OrientDBConnectionKeys.CFGKEY_POOL_SIZE, getPoolSize());
		LOGGER.info("=2===saveConnection===");	
		LOGGER.info("=2===saveConnection==="+nodeSettings.keySet());
	}


	public Integer getPoolSize() {
		return poolSize;
	}
	public void setPoolSize(Integer poolSize) {
		this.poolSize = poolSize;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String m_dbUrl) {
		this.dbUrl = m_dbUrl;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getCredName() {
		return credName;
	}

	public void setCredName(String m_credName) {
		this.credName = m_credName;
	}	
	
}
