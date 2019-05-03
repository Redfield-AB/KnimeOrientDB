package se.redfield.node.port.orientdb.connection;

import static se.redfield.node.port.orientdb.connection.OrientDBConnectionNodeModel.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentPasswordField;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

public class OrientDBConnectionNodeDialog extends DefaultNodeSettingsPane {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBConnectionNodeDialog.class);
	private DialogComponentStringSelection componentStringSelection = null;

	@SuppressWarnings("deprecation")
	protected OrientDBConnectionNodeDialog() {
		super();
		

		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(CFGKEY_POOL_SIZE, DEFAULT_POOL_SIZE, MIN_POOL_SIZE, MAX_POOL_SIZE),
				"Pool size:", /* step */ 1, /* componentwidth */ 5));
		addDialogComponent(
				new DialogComponentString(new SettingsModelString(CFGKEY_DB_URL, DEFAULT_DB_URL), CFGKEY_DB_URL));
		addDialogComponent(new DialogComponentString(
				new SettingsModelString(CFGKEY_REMOTE_DATABASE_NAME, DEFAULT_DB_NAME), CFGKEY_REMOTE_DATABASE_NAME));
		addDialogComponent(new DialogComponentString(new SettingsModelString(CFGKEY_USER_NAME, DEFAULT_USERNAME),
				CFGKEY_USER_NAME));
		addDialogComponent(new DialogComponentPasswordField(new SettingsModelString(CFGKEY_PASSWORD, DEFAULT_PASSWORD),
				CFGKEY_PASSWORD));
		componentStringSelection = new DialogComponentStringSelection(
				new SettingsModelString(CFGKEY_CREDENTIONAL_NAME, DEFAULT_CREDENTIONAL_NAME), CFGKEY_CREDENTIONAL_NAME,
				Arrays.asList("1","2"), false);
		addDialogComponent(componentStringSelection);

	}
	

	@Override
	public void loadAdditionalSettingsFrom(NodeSettingsRO settings, PortObjectSpec[] specs)
			throws NotConfigurableException {
		// TODO Auto-generated method stub
		super.loadAdditionalSettingsFrom(settings, specs);
		logger.info("1. getCredentialsNames: "+getCredentialsNames());
		List<String> items = new LinkedList<>();
		items.add(DEFAULT_CREDENTIONAL_NAME);
		items.addAll(getCredentialsNames());
		componentStringSelection.replaceListItems(items, settings.getString(CFGKEY_CREDENTIONAL_NAME, null));
	}


	

}
