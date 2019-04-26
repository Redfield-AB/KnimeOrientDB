package se.redfield.node.port.orientdb.command;

import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.CFGKEY_CLASS;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.CFGKEY_COLUMN_WITH_COMMAND;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.CFGKEY_FIELDS;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.CFGKEY_MODE;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.USE_COMMAND_FROM_TABLE;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.USE_DIRECT_COMMAND;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.WRITE_TABLE_FOR_CLASS;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.CFGKEY_UPSERT;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.CFGKEY_EXECUTION;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.USE_PARALLEL_EXECUTION;
import static se.redfield.node.port.orientdb.command.OrientDBCommandNodeModel.USE_SEQUENT_EXECUTION;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.GroupLayout;
import javax.swing.JEditorPane;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.ScrollPaneConstants;

import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ColumnFilter;
import org.knime.core.node.util.FlowVariableListCellRenderer;
import org.knime.core.node.workflow.FlowVariable;

import com.orientechnologies.orient.core.metadata.schema.OClass;

import se.redfield.node.port.orientdb.AbstractOrientDBNodeDialogPane;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObjectSpec;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionSettings;

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
public class OrientDBCommandNodeDialog extends AbstractOrientDBNodeDialogPane {

	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBCommandNodeDialog.class);
	private OrientDBConnectionSettings connectionSettings;
	private static final ClassFilter CLASS_FILTER = new ClassFilter();
	private DialogComponentButtonGroup selectModeGroup;
	private DialogComponentButtonGroup selectExecutionGroup;
	private DialogComponentColumnNameSelection columnNameSelection;
	private DialogComponentStringSelection classNameSelection;
	private DialogComponentBoolean useUpsert;
	private DialogComponentColumnFilter componentColumnFilter;

	private DefaultListModel<FlowVariable> m_listModelVars;
	private JList<FlowVariable> m_listVars;

	private final JEditorPane m_statmnt = new JEditorPane("text", "");

	protected OrientDBCommandNodeDialog() throws InvalidSettingsException {
		this.selectModeGroup = new DialogComponentButtonGroup(new SettingsModelString(CFGKEY_MODE, USE_DIRECT_COMMAND),
				false, CFGKEY_MODE, USE_COMMAND_FROM_TABLE, USE_DIRECT_COMMAND, WRITE_TABLE_FOR_CLASS);
		this.selectExecutionGroup = new DialogComponentButtonGroup(new SettingsModelString(CFGKEY_EXECUTION, USE_PARALLEL_EXECUTION),
				false, CFGKEY_EXECUTION, USE_PARALLEL_EXECUTION, USE_SEQUENT_EXECUTION);
		
		getDialogComponents().add(selectModeGroup);
		getDialogComponents().add(selectExecutionGroup);
		JPanel selectModePanel = selectModeGroup.getComponentPanel();
		JPanel selectExecutionPanel = selectExecutionGroup.getComponentPanel();

		this.columnNameSelection = new DialogComponentColumnNameSelection(
				new SettingsModelString(CFGKEY_COLUMN_WITH_COMMAND, null), CFGKEY_COLUMN_WITH_COMMAND,
				OrientDBCommandNodeModel.DATA_TABLE_INDEX, false, new DefaultColumnFilter());
		getDialogComponents().add(columnNameSelection);
		JPanel selectColumnPanel = columnNameSelection.getComponentPanel();

		JPanel commandPanel = createCommandPanel();

		JPanel selectClassFieldsPanel = createSelectClassFieldsPanel();

		JPanel settingPanel = new JPanel();
		GroupLayout settingPanetLayout = new GroupLayout(settingPanel);
		settingPanel.setLayout(settingPanetLayout);
		settingPanetLayout.setHorizontalGroup(settingPanetLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
				.addComponent(selectModePanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				.addComponent(selectExecutionPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				.addComponent(selectColumnPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				.addComponent(commandPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				.addComponent(selectClassFieldsPanel, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE,
						Short.MAX_VALUE));
		settingPanetLayout
				.setVerticalGroup(settingPanetLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addGroup(settingPanetLayout.createSequentialGroup()
								.addComponent(selectModePanel, GroupLayout.PREFERRED_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
								.addGap(2, 2, 10)
								.addComponent(selectExecutionPanel, GroupLayout.PREFERRED_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
								.addGap(5, 5, 15)
								.addComponent(selectColumnPanel, javax.swing.GroupLayout.PREFERRED_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
								.addGap(5, 5, 15)
								.addComponent(commandPanel, javax.swing.GroupLayout.PREFERRED_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(selectClassFieldsPanel, javax.swing.GroupLayout.PREFERRED_SIZE,
										javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)));

		super.addTab("Settings", settingPanel, true);
		addListeners();
	}

	private JPanel createSelectClassFieldsPanel() throws InvalidSettingsException {
		final JPanel selectClassFieldsPanel = new JPanel();
		GroupLayout selectClassFieldsPanelLayout = new GroupLayout(selectClassFieldsPanel);
		selectClassFieldsPanel.setLayout(selectClassFieldsPanelLayout);
		this.classNameSelection = new DialogComponentStringSelection(new SettingsModelString(CFGKEY_CLASS, null),
				CFGKEY_CLASS, getAvailableClasses(CLASS_FILTER));
		this.useUpsert = new DialogComponentBoolean(new SettingsModelBoolean(CFGKEY_UPSERT, true), CFGKEY_UPSERT);
		getDialogComponents().add(classNameSelection);
		getDialogComponents().add(useUpsert);
		this.componentColumnFilter = new DialogComponentColumnFilter(new SettingsModelFilterString(CFGKEY_FIELDS), 
				OrientDBCommandNodeModel.DATA_TABLE_INDEX,
				true);
		getDialogComponents().add(componentColumnFilter);
		JPanel panel1 = new JPanel();
		GroupLayout groupLayout = new GroupLayout(panel1);
		panel1.setLayout(groupLayout);
		
		groupLayout
				.setHorizontalGroup(groupLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
								.addComponent(classNameSelection.getComponentPanel(), GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE,
										Short.MAX_VALUE)
								.addComponent(useUpsert.getComponentPanel(), GroupLayout.DEFAULT_SIZE,
										GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)));

		groupLayout.setVerticalGroup(groupLayout.createParallelGroup()
				.addComponent(classNameSelection.getComponentPanel(), GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				.addComponent(useUpsert.getComponentPanel(), GroupLayout.DEFAULT_SIZE,
						GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE));
		
		selectClassFieldsPanelLayout
				.setVerticalGroup(selectClassFieldsPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addGroup(selectClassFieldsPanelLayout.createSequentialGroup()
								.addComponent(panel1, GroupLayout.DEFAULT_SIZE,
										GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
								.addComponent(componentColumnFilter.getComponentPanel(), GroupLayout.DEFAULT_SIZE,
										GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)));

		selectClassFieldsPanelLayout.setHorizontalGroup(selectClassFieldsPanelLayout.createParallelGroup()
				.addComponent(panel1, GroupLayout.DEFAULT_SIZE,
						GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				.addComponent(componentColumnFilter.getComponentPanel(), GroupLayout.DEFAULT_SIZE,
						GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE));
		return selectClassFieldsPanel;
	}

	private JPanel createCommandPanel() {
		JPanel commandPanel = new JPanel();
		GroupLayout commandPanelLayout = new GroupLayout(commandPanel);
		commandPanel.setLayout(commandPanelLayout);

		m_listModelVars = new DefaultListModel<FlowVariable>();
		m_listVars = new JList<FlowVariable>(m_listModelVars);
		m_listVars.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		m_listVars.setCellRenderer(new FlowVariableListCellRenderer());

		JScrollPane queryScrollPane = new JScrollPane(m_statmnt, ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
				ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		queryScrollPane.setBorder(BorderFactory.createTitledBorder(" SQL Statement "));

		final JScrollPane scrollVars = new JScrollPane(m_listVars, ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS,
				ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scrollVars.setBorder(BorderFactory.createTitledBorder(" Flow Variable List "));
		JSplitPane jsp = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
		jsp.setResizeWeight(0.2);
		jsp.setLeftComponent(scrollVars);
		jsp.setRightComponent(queryScrollPane);

		commandPanelLayout.setVerticalGroup(commandPanelLayout.createSequentialGroup().addComponent(jsp,
				GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE));
		commandPanelLayout.setHorizontalGroup(commandPanelLayout.createSequentialGroup().addComponent(jsp,
				GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE));

		m_listVars.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(java.awt.event.MouseEvent e) {
				if (e.getClickCount() == 2) {
					Object o = m_listVars.getSelectedValue();
					if (o != null) {
						FlowVariable var = (FlowVariable) o;
						m_statmnt.replaceSelection(FlowVariableResolver.getPlaceHolderForVariable(var));
						m_listVars.clearSelection();
						m_statmnt.requestFocus();
					}
				}
			}
		});
		return commandPanel;
	}

	private void addListeners() {
		this.selectModeGroup.getButton(USE_COMMAND_FROM_TABLE).addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				logger.info("actionPerformed " + e);
				columnNameSelection.setEnabled(true);

				m_listVars.setEnabled(false);
				m_statmnt.setEnabled(false);

				classNameSelection.setEnabled(false);
				useUpsert.setEnabled(false);
				componentColumnFilter.setEnabled(false);
				selectExecutionGroup.setEnabled(true);
			}
		});
		this.selectModeGroup.getButton(USE_DIRECT_COMMAND).addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				logger.info("actionPerformed " + e);
				columnNameSelection.setEnabled(false);

				m_listVars.setEnabled(true);
				m_statmnt.setEnabled(true);

				classNameSelection.setEnabled(false);
				useUpsert.setEnabled(false);
				componentColumnFilter.setEnabled(false);
				selectExecutionGroup.setEnabled(false);
			}
		});
		this.selectModeGroup.getButton(WRITE_TABLE_FOR_CLASS).addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				logger.info("actionPerformed " + e);
				columnNameSelection.setEnabled(false);

				m_listVars.setEnabled(false);
				m_statmnt.setEnabled(false);

				classNameSelection.setEnabled(true);
				useUpsert.setEnabled(true);
				componentColumnFilter.setEnabled(true);
				selectExecutionGroup.setEnabled(true);
			}
		});
	}

	@Override
	public void onOpen() {
		String modeName = ((SettingsModelString) selectModeGroup.getModel()).getStringValue();
		AbstractButton button = this.selectModeGroup.getButton(modeName);
		if (button!=null) {
			button.doClick();
		} else {
			logger.warn("Button for '"+modeName+"' not found!");
		}
	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) throws InvalidSettingsException {
		for (DialogComponent comp : getDialogComponents()) {
			comp.saveSettingsTo(settings);
		}
		settings.addString(OrientDBCommandNodeModel.CFGKEY_COMMAND, m_statmnt.getText());

	}

	@Override
	public final void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
			throws NotConfigurableException {
		
		if (specs == null || specs[OrientDBCommandNodeModel.ORIENTDB_CONNECTION_INDEX] == null) {
			throw new NotConfigurableException("No required input available");
		}
		OrientDBConnectionPortObjectSpec spec = (OrientDBConnectionPortObjectSpec) specs[OrientDBCommandNodeModel.ORIENTDB_CONNECTION_INDEX];

		try {
			this.connectionSettings = spec.getConnectionSettings(getCredentialsProvider());
		} catch (InvalidSettingsException e) {
			throw new NotConfigurableException("Cannot get connection settings", e);
		}
		logger.info(" (connectionSettings!=null) :" + (connectionSettings != null));
		
		try {
			classNameSelection.replaceListItems(getAvailableClasses(CLASS_FILTER), settings.getString(CFGKEY_CLASS, null));
		} catch (InvalidSettingsException e1) {
			throw new NotConfigurableException("Cannot load classes!", e1);
		}
		
		m_listModelVars.removeAllElements();
		for (Map.Entry<String, FlowVariable> e : getAvailableFlowVariables().entrySet()) {
			m_listModelVars.addElement(e.getValue());
		}
		for (DialogComponent comp : getDialogComponents()) {
			comp.loadSettingsFrom(settings, specs);
		}
		m_statmnt.setText(settings.getString(OrientDBCommandNodeModel.CFGKEY_COMMAND, ""));
		boolean hasInputTable = (specs.length > 0 && specs[OrientDBCommandNodeModel.DATA_TABLE_INDEX] != null);

		// user uses input data table
		selectModeGroup.getButton(WRITE_TABLE_FOR_CLASS).setVisible(hasInputTable);
		selectModeGroup.getButton(USE_COMMAND_FROM_TABLE).setVisible(hasInputTable);
	}
	
	private static class ClassFilter implements Predicate<OClass> {
		private static final List<String> SYSTEM_NAMES = Arrays.asList("OSchedule", "OSequence", "OFunction");

		@Override
		public boolean test(OClass oClass) {
			boolean hasName = !oClass.getName().isEmpty();
			boolean realClass = !oClass.isAbstract();
			boolean isVertexOrDocument = (oClass.isSubClassOf("V") || oClass.getSuperClasses().isEmpty());
			boolean isNotSystemClass = !SYSTEM_NAMES.contains(oClass.getName());
			boolean isNotEdgeClass = !oClass.isSubClassOf("E");
			return hasName && realClass && isVertexOrDocument && isNotSystemClass && isNotEdgeClass;
		}

	}

	private static class DefaultColumnFilter implements ColumnFilter {

		@Override
		public boolean includeColumn(DataColumnSpec colSpec) {
			return true;
		}

		@Override
		public String allFilteredMsg() {
			return "allFilteredMsg";
		}

	}

	@Override
	protected OrientDBConnectionSettings getConnectionSettings() {
		return connectionSettings;
	}	

}
