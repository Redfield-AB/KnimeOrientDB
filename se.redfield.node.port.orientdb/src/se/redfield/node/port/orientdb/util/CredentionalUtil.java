package se.redfield.node.port.orientdb.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.util.KnimeEncryption;

import se.redfield.node.port.orientdb.connection.OrientDBConnectionNodeModel;

public class CredentionalUtil {
	private static final NodeLogger logger = NodeLogger.getLogger(CredentionalUtil.class);
	private static final Pattern BASE64_PATTERN = Pattern
			.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

	public static UserLogin getUserLoginInfo(String userName, String password, String credentionalName,
			CredentialsProvider credentialsProvider) throws InvalidSettingsException {
		String decryptedPassword = null;
		String login = null;
		if ( (userName==null && password==null ) || (credentionalName==null)) {
			throw new InvalidSettingsException("No data for login to database! Please configure username and password or credentional!");			
		}

		if (OrientDBConnectionNodeModel.DEFAULT_CREDENTIONAL_NAME.equals(credentionalName)) {
			if (isBase64(password)) {
				try {
					decryptedPassword = KnimeEncryption.decrypt(password);
				} catch (Exception e) {
					throw new InvalidSettingsException("Password could not be decrypted!", e);
				}
			} else {
				decryptedPassword = password;
			}

			login = userName;
		} else {
			ICredentials credentional = credentialsProvider.get(credentionalName);
			login = credentional.getLogin();
			decryptedPassword = credentional.getPassword();
		}
		return new UserLogin(decryptedPassword, login);

	}

	private static boolean isBase64(String string) {
		Matcher matcher = BASE64_PATTERN.matcher(string);
		return matcher.find();
	}

	public static class UserLogin {
		private String decryptedPassword;
		private String login;

		public UserLogin(String decryptedPassword, String login) {
			super();
			this.decryptedPassword = decryptedPassword;
			this.login = login;
		}

		public String getDecryptedPassword() {
			return decryptedPassword;
		}

		public String getLogin() {
			return login;
		}

	}

}
