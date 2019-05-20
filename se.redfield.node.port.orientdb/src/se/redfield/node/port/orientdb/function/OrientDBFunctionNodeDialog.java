package se.redfield.node.port.orientdb.function;

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.GroupLayout.ParallelGroup;
import javax.swing.GroupLayout.SequentialGroup;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSplitPane;
import javax.swing.JTextField;
import javax.swing.LayoutStyle;
import javax.swing.Spring;
import javax.swing.SpringLayout;
import javax.swing.border.Border;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObjectSpec;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionSettings;
import se.redfield.node.port.orientdb.execute.OrientDBExecuteNodeModel;
import se.redfield.node.port.orientdb.util.CredentionalUtil;

/**
 * <code>NodeDialog</code> for the "OrientDBNodeTest" Node.
 * 
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBFunctionNodeDialog extends NodeDialogPane {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBFunctionNodeDialog.class);
	private static final Color BLACK_COLOR = new java.awt.Color(0, 0, 0);
	@Deprecated
	private static final Map<String, JTextField> FUNCTION_PARAM_TEXT_FIELDS = new ConcurrentHashMap<>();
	private static final Map<String, JComboBox<String>> FUNCTION_PARAM_LIST_FIELDS = new ConcurrentHashMap<>();
	private static final Set<String> OLD_FUNCTION_PARAM_FIELD_NAMES = new HashSet<>();

	private OrientDBConnectionSettings connectionSettings;

	private DefaultListModel<String> functionsListModel;
	private JList<String> functionsList;
	private JPanel parametersPanel;
	private JScrollPane functionsScrollPane;
	private JSplitPane functionSplitPane;
	private JSplitPane centralSplitPane;
	private JCheckBox loadDocuments;
	private boolean existsInputTable = false;
	private boolean existsInputFlowVariables = false;
	private List<String> inputTableColumns = new LinkedList<>();
	private final List<FlowVariable> flowVariables = new LinkedList<FlowVariable>();

	protected OrientDBFunctionNodeDialog() {
		super();
		initComponents();
	}

	private void initComponents() {

		JPanel settingPanel = new JPanel();
		GroupLayout settingPanetLayout = new GroupLayout(settingPanel);
		settingPanel.setLayout(settingPanetLayout);
		settingPanel.setPreferredSize(new Dimension(550, 250));

		centralSplitPane = new javax.swing.JSplitPane();
		functionSplitPane = new javax.swing.JSplitPane();
		functionsScrollPane = new javax.swing.JScrollPane();
		functionsList = new javax.swing.JList<>();
		loadDocuments = new JCheckBox("Load documents", true);
		parametersPanel = new javax.swing.JPanel();
		parametersPanel.setPreferredSize(new Dimension(250, 250));

		centralSplitPane.setDividerLocation(200);
		functionSplitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
		functionSplitPane.setDividerLocation(100);

		functionsScrollPane
				.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(BLACK_COLOR), "Functions"));
		functionsListModel = new DefaultListModel<String>();
		functionsList.setModel(functionsListModel);
		functionsList.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
		functionsScrollPane.setViewportView(functionsList);
		
		functionSplitPane.setRightComponent(functionsScrollPane);

		centralSplitPane.setLeftComponent(functionsScrollPane);
		parametersPanel.setLayout(new BoxLayout(parametersPanel, BoxLayout.Y_AXIS));
		centralSplitPane.setRightComponent(parametersPanel);

		settingPanetLayout.setHorizontalGroup(settingPanetLayout
				.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(settingPanetLayout.createSequentialGroup().addContainerGap()
						.addComponent(centralSplitPane, javax.swing.GroupLayout.DEFAULT_SIZE, 552, Short.MAX_VALUE)
						.addContainerGap()));
		settingPanetLayout.setVerticalGroup(
				settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
						settingPanetLayout.createSequentialGroup().addContainerGap().addComponent(centralSplitPane,
								javax.swing.GroupLayout.DEFAULT_SIZE, 428, Short.MAX_VALUE)));
		super.addTab("Settings", settingPanel, true);
		functionsList.addListSelectionListener(new ListSelectionListener() {

			@Override
			public void valueChanged(ListSelectionEvent e) {
				if (!e.getValueIsAdjusting()) {
					logger.infoWithFormat("functionsList.getSelectedValuesList(): %s",
							functionsList.getSelectedValue());
					String funcNameWithParameters = functionsList.getSelectedValue();
					if (funcNameWithParameters != null) {
						parametersPanel.removeAll();
						List<String> parameters = Utils.extractParameterNames(funcNameWithParameters);
						OLD_FUNCTION_PARAM_FIELD_NAMES.addAll(FUNCTION_PARAM_LIST_FIELDS.keySet());

						FUNCTION_PARAM_LIST_FIELDS.clear();
						parametersPanel.removeAll();
						JPanel functionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
						functionPanel.setMaximumSize(new Dimension(300, 30));
						loadDocuments.setMaximumSize(new Dimension(300, 28));
						functionPanel.add(loadDocuments);
						parametersPanel.add(functionPanel);
						
						for (String parameter : parameters) {
							JPanel rowPanel = new JPanel();
							GroupLayout rowPanelLayout = new GroupLayout(rowPanel);
							rowPanel.setLayout(rowPanelLayout);
							rowPanel.setPreferredSize(new Dimension(300, 30));

							JLabel label = new JLabel(parameter, JLabel.RIGHT);
							label.setSize(100, 35);
							rowPanel.add(label);
							JComboBox<String> columnMaping = new JComboBox<>();
							columnMaping.removeAllItems();
							columnMaping.setSize(200, 35);
							if (existsInputFlowVariables && !existsInputTable) {
								for (FlowVariable flowVariable : flowVariables) {
									columnMaping.addItem(flowVariable.getName());
								}
							} else  if (!existsInputFlowVariables && existsInputTable) {
								for (String columnName : inputTableColumns) {
									columnMaping.addItem(columnName);
								}
							} else {
								logger.info("use flow variable or table!"); 
							}
							
							FUNCTION_PARAM_LIST_FIELDS.put(parameter, columnMaping);
				
							rowPanel.add(columnMaping);
							rowPanel.setBorder(BorderFactory.createLineBorder(BLACK_COLOR));
							rowPanelLayout.setHorizontalGroup(
									rowPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
							            .addGroup(rowPanelLayout.createSequentialGroup()
							                .addGap(5, 5, 5)
							                .addComponent(label, 100, 100, 220)
							                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
							                .addComponent(columnMaping, 200, 220, Short.MAX_VALUE)
							                .addGap(5, 5, 5))
							        );
							rowPanelLayout.setVerticalGroup(
									rowPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
							            .addGroup(rowPanelLayout.createSequentialGroup()
							                .addGap(5, 5, 5)
							                .addGroup(rowPanelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
							                    .addComponent(label, GroupLayout.DEFAULT_SIZE, 29, 50)
							                    .addComponent(columnMaping, GroupLayout.DEFAULT_SIZE, 29, 50))
							                .addGap(5, 5, 5))
							        );

							parametersPanel.add(rowPanel);

						}

						parametersPanel.revalidate();
						parametersPanel.repaint();
					}
				}
			}
		});

	}

	@Override
	protected void loadSettingsFrom(NodeSettingsRO settings, PortObjectSpec[] specs) throws NotConfigurableException {
		logger.info("=!== loadSettingsFrom (PortObjectSpec) ====");
		logger.info("=!== loadSettingsFrom settings.keySet() :"+settings.keySet());
		if (specs == null || specs[OrientDBFunctionNodeModel.ORIENTDB_CONNECTION_INDEX] == null) {
			throw new NotConfigurableException("No required input available");
		}
		
		existsInputFlowVariables =  (specs[OrientDBFunctionNodeModel.FLOW_VARIABLE_INDEX] != null);
		existsInputTable = (specs.length > OrientDBFunctionNodeModel.DATA_TABLE_INDEX
				&& specs[OrientDBFunctionNodeModel.DATA_TABLE_INDEX] != null);
		
		OrientDBConnectionPortObjectSpec spec = (OrientDBConnectionPortObjectSpec) specs[OrientDBFunctionNodeModel.ORIENTDB_CONNECTION_INDEX];
		
		flowVariables.clear();
		for (Map.Entry<String, FlowVariable> e : getAvailableFlowVariables().entrySet()) {
			flowVariables.add(e.getValue());
		}

		try {
			this.connectionSettings = spec.getConnectionSettings(getCredentialsProvider());
		} catch (InvalidSettingsException e) {
			throw new NotConfigurableException("OrientDB classes haven't loaded from database!", e);
		}
		functionsListModel.removeAllElements();
		try {
			for (String functionWithParams : getAvailableFunctions()) {
				functionsListModel.addElement(functionWithParams);
			}
		} catch (InvalidSettingsException e1) {
			throw new NotConfigurableException("OrientDB functions haven't loaded from database!", e1);
		}
		if (specs.length > OrientDBFunctionNodeModel.DATA_TABLE_INDEX
				&& specs[OrientDBFunctionNodeModel.DATA_TABLE_INDEX] != null) {
			DataTableSpec tableTableSpec = (DataTableSpec) specs[OrientDBFunctionNodeModel.DATA_TABLE_INDEX];
			this.inputTableColumns.clear();
			this.inputTableColumns.addAll(Arrays.asList(tableTableSpec.getColumnNames()));
		} else {
			this.inputTableColumns.clear();
		}

		try {
			String functionName = settings.getString(OrientDBFunctionNodeModel.CFGKEY_FUNCTION);
			String[] functionParamNames = settings
					.getStringArray(OrientDBFunctionNodeModel.CFGKEY_FUNCTION_PARAM_NAMES);
			String fullFunctionName = functionName + "["
					+ Arrays.stream(functionParamNames).collect(Collectors.joining(",")) + "]";
			logger.info("fullFunctionName :"+fullFunctionName);
			int selectedIndex = findIndex(fullFunctionName);
			functionsList.setSelectedIndex(selectedIndex);
			logger.info("selectedIndex :"+selectedIndex);
			for (String functionParamName : functionParamNames) {
					JComboBox<String> compoBox =  FUNCTION_PARAM_LIST_FIELDS.get(functionParamName);
					int compoBoxSelectedIndex =findIndex(compoBox, settings.getString(functionParamName, null));
					logger.info("compoBoxSelectedIndex :"+compoBoxSelectedIndex);
					compoBox.setSelectedIndex(compoBoxSelectedIndex);
				
			}
			loadDocuments.setSelected(settings.getBoolean(OrientDBFunctionNodeModel.CFGKEY_LOAD_DOC, false));
		} catch (InvalidSettingsException e) {
			throw new NotConfigurableException("Settings haven't loaded", e);
		}
		

	}
	
	private int findIndex(JComboBox<String> compoBox, String selectedValue) {
		logger.info("selectedValue :"+selectedValue);
		int index = 0;
		for (int i=0 ; i<compoBox.getItemCount();i++ ) {
			String currentItemValue = compoBox.getItemAt(i);
			logger.info("currentItemValue :"+currentItemValue+"; index = "+i);
			if (currentItemValue.equals(selectedValue)) {
				index = i;
				break;				
			}			
		}
		return index;		
	}
	
	private int findIndex(String fullFunctionName) {
		int index = 0;
		for (int i=0 ; i<functionsListModel.size();i++ ) {
			String currentFuncName = functionsListModel.get(i);
			logger.info("currentFuncName :"+currentFuncName+"; index = "+i);
			if (currentFuncName.equals(fullFunctionName)) {
				index = i;
				break;				
			}
		}
		return index;
	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) throws InvalidSettingsException {
		String functionName = Utils.extractName(functionsList.getSelectedValue());
		logger.infoWithFormat("selected function : %s", functionName);
		settings.addString(OrientDBFunctionNodeModel.CFGKEY_FUNCTION, functionName);
		settings.addBoolean(OrientDBFunctionNodeModel.CFGKEY_LOAD_DOC, this.loadDocuments.isSelected());
	
		for (Map.Entry<String, JComboBox<String>> entry : FUNCTION_PARAM_LIST_FIELDS.entrySet()) {
			logger.infoWithFormat("1. function param %s. value: %s", entry.getKey(),
					entry.getValue().getSelectedIndex());
			JComboBox<String> compoBox = entry.getValue();
			int selectedIndex = compoBox.getSelectedIndex();
			String selectedValue = compoBox.getItemAt(selectedIndex);
			logger.infoWithFormat("1. selectedIndex %s. selectedValue: %s", selectedIndex, selectedValue);
			settings.addString(entry.getKey(), selectedValue);
		}
		settings.addStringArray(OrientDBFunctionNodeModel.CFGKEY_FUNCTION_PARAM_NAMES,
				FUNCTION_PARAM_LIST_FIELDS.keySet().toArray(new String[FUNCTION_PARAM_LIST_FIELDS.size()]));

	}

	private Collection<String> getAvailableFunctions() throws InvalidSettingsException {
		logger.info("=!== getAvailableFunctions ====");
		if (getConnectionSettings() == null) {
			logger.warn("no connection info. Go out!");
			return Collections.singletonList("==no functions!==");
		}

		Set<String> availableFunctions = new HashSet<String>();
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(getConnectionSettings().getUserName(), getConnectionSettings().getPassword(),
				getConnectionSettings().getCredName(), getCredentialsProvider());
		
		OrientDB orientDBEnv = new OrientDB(getConnectionSettings().getDbUrl(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv, getConnectionSettings().getDbName(),
				userLogin.getLogin(), userLogin.getDecryptedPassword());


		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			try (OResultSet resultSet = databaseSession.query("select from OFunction")) {
				while (resultSet.hasNext()) {
					OResult result = resultSet.next();
					String name = result.getProperty("name");
					logger.info("=!== function name:" + name);
					OTrackedList<String> parameters = result.getProperty("parameters");
					if (parameters != null) {
						availableFunctions.add(name.toUpperCase(Locale.ENGLISH) + "["
								+ parameters.stream().collect(Collectors.joining(",")) + "]");
					} else {
						availableFunctions.add(name.toUpperCase(Locale.ENGLISH) + "[]");
					}
				}

			}
		} finally {
			orientDBPool.close();
			orientDBEnv.close();
		}
		return availableFunctions;
	}

	private OrientDBConnectionSettings getConnectionSettings() {
		return this.connectionSettings;
	}

}
