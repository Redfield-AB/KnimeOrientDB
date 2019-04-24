package se.redfield.node.port.orientdb.execute;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.GroupLayout;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;

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
public class OrientDBExecuteNodeDialog extends AbstractOrientDBNodeDialogPane {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBExecuteNodeDialog.class);
	private static final ClassFilter CLASS_FILTER = new ClassFilter();
	public static final String CFGKEY_SELECTED_FLOW_VARIABLES = "SELECTED_FLOW_VARIABLES";
	public static final String CFGKEY_SELECTED_SCHEMA_CLASSES = "SELECTED_CLASSES";

	private OrientDBConnectionSettings connectionSettings;

	private javax.swing.JTextArea batchScriptTextArea;
	private JSpinner batchSizeSpinner;
	private DefaultListModel<String> classesListModel;
	private JList<String> classesList;
	private DefaultListModel<String> fieldsListModel;
	private JList<String> fieldsList;
	private DefaultListModel<String> flowVariablesModel;
	private JList<String> flowVariablesList;
	private JCheckBox generateByTemplateCheckBox;
	private JScrollPane classesListScrollPane1;
	private JScrollPane jScrollPane3;
	private javax.swing.JScrollPane fieldsListScrollPane;
	private javax.swing.JSplitPane centralSplitPane1;
	private javax.swing.JSplitPane classesFieldsSplitPane2;
	private javax.swing.JSplitPane fvTableSplitPane;
	private DefaultListModel<String> tableColumnsListModel;
	private javax.swing.JList<String> tableColumnsList;
	
	private JTextField returnConstructionTextField;

	protected OrientDBExecuteNodeDialog() {
		super();
		initComponents();
	}

	private void initComponents() {
		JPanel settingPanel = new JPanel();
		GroupLayout settingPanetLayout = new GroupLayout(settingPanel);
		settingPanel.setLayout(settingPanetLayout);
		settingPanel.setPreferredSize(new Dimension(938, 414));

		centralSplitPane1 = new JSplitPane();
		JSplitPane fvClassSplitPane = new JSplitPane();
		classesFieldsSplitPane2 = new JSplitPane();
		classesListScrollPane1 = new JScrollPane();
		classesList = new JList<>();
		fieldsListScrollPane = new JScrollPane();
		fieldsList = new JList<>();
		fvTableSplitPane = new JSplitPane();
		JScrollPane tableColumnsScrollPane = new JScrollPane();
		tableColumnsList = new JList<>();
		JScrollPane flowVariablesScrollPane7 = new JScrollPane();
		flowVariablesList = new JList<>();
		javax.swing.JPanel rightPanel = new JPanel();
		generateByTemplateCheckBox = new JCheckBox();
		jScrollPane3 = new JScrollPane();
		batchScriptTextArea = new JTextArea();
		batchSizeSpinner = new JSpinner(new SpinnerNumberModel(OrientDBExecuteNodeModel.MAX_BATCH_SIZE,1,null,1 ));
		JLabel batchSizeLabel = new JLabel();
		JLabel returnConstructionLabel = new JLabel();
		returnConstructionTextField = new JTextField();

		centralSplitPane1.setDividerLocation(200);

		fvClassSplitPane.setDividerLocation(0.5);
		fvClassSplitPane.setOrientation(javax.swing.JSplitPane.VERTICAL_SPLIT);

		classesListScrollPane1.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black),"Classes"));
		classesListModel = new DefaultListModel<String>();
		classesList.setModel(classesListModel);
		classesList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		classesListScrollPane1.setViewportView(classesList);

		classesFieldsSplitPane2.setLeftComponent(classesListScrollPane1);

		fieldsListScrollPane.setBorder(BorderFactory
				.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Fields"));
		fieldsListModel = new DefaultListModel<String>();
		fieldsList.setModel(fieldsListModel);
		fieldsList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		fieldsListScrollPane.setViewportView(fieldsList);

		classesFieldsSplitPane2.setRightComponent(fieldsListScrollPane);
		classesFieldsSplitPane2.setDividerLocation(100);

		fvClassSplitPane.setRightComponent(classesFieldsSplitPane2);

		tableColumnsScrollPane.setBorder(BorderFactory
				.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Table columns"));
		tableColumnsListModel = new DefaultListModel<String>();
		tableColumnsList.setModel(tableColumnsListModel);
		tableColumnsScrollPane.setViewportView(tableColumnsList);

		fvTableSplitPane.setRightComponent(tableColumnsScrollPane);

		flowVariablesScrollPane7.setBorder(BorderFactory
				.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Flow variables"));
		flowVariablesModel = new DefaultListModel<String>();
		flowVariablesList.setModel(flowVariablesModel);
		flowVariablesScrollPane7.setViewportView(flowVariablesList);

		fvTableSplitPane.setLeftComponent(flowVariablesScrollPane7);
		fvTableSplitPane.setDividerLocation(100);
		fvClassSplitPane.setLeftComponent(fvTableSplitPane);

		centralSplitPane1.setLeftComponent(fvClassSplitPane);

		generateByTemplateCheckBox.setText("Generate by template");

		jScrollPane3.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black),
				"Batch script"));

		batchScriptTextArea.setColumns(20);
		batchScriptTextArea.setRows(5);
		jScrollPane3.setViewportView(batchScriptTextArea);

		batchSizeLabel.setText("Batch size limit");
        returnConstructionLabel.setText("Return construction :");
		JPanel returnConstructionPanel = new JPanel();
		returnConstructionPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		javax.swing.GroupLayout returnConstructionPanelLayout = new javax.swing.GroupLayout(returnConstructionPanel);
		returnConstructionPanel.setLayout(returnConstructionPanelLayout);
		returnConstructionPanelLayout.setHorizontalGroup(
				returnConstructionPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(returnConstructionPanelLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(returnConstructionPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                    .addGroup(returnConstructionPanelLayout.createSequentialGroup()
                        .addComponent(returnConstructionLabel, GroupLayout.PREFERRED_SIZE, 201, GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 0, Short.MAX_VALUE))
                    .addComponent(returnConstructionTextField))
                .addContainerGap())
        );
		returnConstructionPanelLayout.setVerticalGroup(
				returnConstructionPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		            .addGroup(returnConstructionPanelLayout.createSequentialGroup()
		                .addContainerGap()
		                .addComponent(returnConstructionLabel)
		                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
		                .addComponent(returnConstructionTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
		                .addContainerGap())
		        );
		
		javax.swing.GroupLayout rightPanelLayout = new javax.swing.GroupLayout(rightPanel);
        rightPanel.setLayout(rightPanelLayout);
        rightPanelLayout.setHorizontalGroup(
            rightPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(rightPanelLayout.createSequentialGroup()
                .addGroup(rightPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(rightPanelLayout.createSequentialGroup()
                        .addContainerGap()
                        .addComponent(jScrollPane3, javax.swing.GroupLayout.DEFAULT_SIZE, 700, Short.MAX_VALUE))
                    .addGroup(rightPanelLayout.createSequentialGroup()
                        .addGap(14, 14, 14)
                        .addComponent(generateByTemplateCheckBox, javax.swing.GroupLayout.PREFERRED_SIZE, 210, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(18, 18, 18)
                        .addComponent(batchSizeLabel, javax.swing.GroupLayout.PREFERRED_SIZE, 108, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(batchSizeSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, 77, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 73, Short.MAX_VALUE))
                    .addGroup(rightPanelLayout.createSequentialGroup()
                        .addContainerGap()
                        .addComponent(returnConstructionPanel, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                .addContainerGap())
        );
        rightPanelLayout.setVerticalGroup(
            rightPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(rightPanelLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(rightPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(generateByTemplateCheckBox)
                    .addComponent(batchSizeSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, 28, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(batchSizeLabel))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane3, javax.swing.GroupLayout.PREFERRED_SIZE, 292, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(returnConstructionPanel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(13, Short.MAX_VALUE))
        );
		
		

		
		centralSplitPane1.setRightComponent(rightPanel);
		
		
		settingPanetLayout
				.setHorizontalGroup(settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
						.addComponent(centralSplitPane1, GroupLayout.Alignment.TRAILING));
		
		settingPanetLayout.setVerticalGroup(
				settingPanetLayout.createParallelGroup(GroupLayout.Alignment.LEADING).addGroup(
						GroupLayout.Alignment.TRAILING,
						settingPanetLayout.createSequentialGroup()
						.addComponent(centralSplitPane1)
						));

		// add listeners
		flowVariablesList.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent evt) {
				if (evt.getClickCount() == 2) {
					Object o = flowVariablesList.getSelectedValue();
					if (o != null) {
						String flowVariableName = (String) o;
						logger.infoWithFormat("mouseClicked:  flowVariableName: %s", flowVariableName);
						FlowVariable var = getAvailableFlowVariables().get(flowVariableName);
						batchScriptTextArea.replaceSelection(FlowVariableResolver.getPlaceHolderForVariable(var));
						flowVariablesList.clearSelection();
						batchScriptTextArea.requestFocus();
					}
				}
			}
		});
		fieldsList.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent evt) {
				if (evt.getClickCount() == 2) {
					Object o = fieldsList.getSelectedValue();
					if (o != null) {
						String fieldName = (String) o;
						logger.infoWithFormat("mouseClicked:  fieldName: %s", fieldName);
						batchScriptTextArea.replaceSelection(fieldName);
						fieldsList.clearSelection();
						batchScriptTextArea.requestFocus();
					}
				}
			}
		});
		classesList.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent evt) {
				if (evt.getClickCount() == 2) {
					Object o = classesList.getSelectedValue();
					if (o != null) {
						String className = (String) o;
						logger.infoWithFormat("mouseClicked:  className: %s", className);
						batchScriptTextArea.replaceSelection(className);
						batchScriptTextArea.requestFocus();
					}
				}
			}
		});
		
		generateByTemplateCheckBox.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				returnConstructionTextField.setText(null);
				returnConstructionTextField.setEnabled(generateByTemplateCheckBox.isSelected());
			}
		});

		super.addTab("Settings", settingPanel, true);
	}

	@Override
	protected void loadSettingsFrom(NodeSettingsRO settings, PortObjectSpec[] specs) throws NotConfigurableException {
		logger.info("=!== loadSettingsFrom (PortObjectSpec) ====");
		if (specs == null || specs[OrientDBExecuteNodeModel.ORIENTDB_CONNECTION_INDEX] == null) {
			throw new NotConfigurableException("No required input available");
		}
		OrientDBConnectionPortObjectSpec spec = (OrientDBConnectionPortObjectSpec) specs[OrientDBExecuteNodeModel.ORIENTDB_CONNECTION_INDEX];

		try {
			this.connectionSettings = spec.getConnectionSettings(getCredentialsProvider());
		} catch (InvalidSettingsException e) {
			throw new NotConfigurableException("Cannot get connection settings", e);
		}
		logger.info(" (connectionSettings!=null) :" + (connectionSettings != null));

		flowVariablesModel.removeAllElements();
		for (Map.Entry<String, FlowVariable> e : getAvailableFlowVariables().entrySet()) {
			flowVariablesModel.addElement(e.getKey());
		}
		classesListModel.removeAllElements();
		Map<String, List<OProperty>> classesWithProperties;
		try {
			classesWithProperties = getAvailableClassesWithFields(CLASS_FILTER);
		} catch (InvalidSettingsException e2) {
			throw new NotConfigurableException("Cannot get available classes from database!",e2);
		}
		for (Map.Entry<String, List<OProperty>> entry : classesWithProperties.entrySet()) {
			classesListModel.addElement(entry.getKey());
		}

		try {
			classesList.setSelectedValue(settings.getString(CFGKEY_SELECTED_SCHEMA_CLASSES),
					true);
		} catch (InvalidSettingsException e1) {
			logger.warn("Can't load values for key " + CFGKEY_SELECTED_SCHEMA_CLASSES, e1);
		}

		// remove listeners
		for (ListSelectionListener listener : classesList.getListSelectionListeners()) {
			classesList.removeListSelectionListener(listener);
		}

		classesList.addListSelectionListener(new ListSelectionListener() {

			@Override
			public void valueChanged(ListSelectionEvent e) {
				if (!e.getValueIsAdjusting()) {
					logger.infoWithFormat("classesList.getSelectedValuesList(): %s", classesList.getSelectedValue());
					String selectedClassName = classesList.getSelectedValue();
					if (selectedClassName != null) {
						fieldsListModel.removeAllElements();
						for (OProperty property : classesWithProperties.get(selectedClassName)) {
							fieldsListModel.addElement(property.getName());
						}
						fieldsList.repaint();
					}
				}
			}
		});

		fieldsListModel.removeAllElements();

		tableColumnsListModel.removeAllElements();

		if (specs.length > OrientDBExecuteNodeModel.DATA_TABLE_INDEX
				&& specs[OrientDBExecuteNodeModel.DATA_TABLE_INDEX] != null) {
			DataTableSpec tableTableSpec = (DataTableSpec) specs[OrientDBExecuteNodeModel.DATA_TABLE_INDEX];
			for (DataColumnSpec columnSpec : tableTableSpec) {
				tableColumnsListModel.addElement(columnSpec.getName());
			}
			
			MouseAdapter tableColumnsListMouseAdapter = new MouseAdapter() {
				public void mouseClicked(MouseEvent evt) {
					if (evt.getClickCount() == 2) {
						Object o = tableColumnsList.getSelectedValue();
						if (o != null) {
							String columnName = (String) o;
							batchScriptTextArea.replaceSelection("$$(" + columnName + ")$$");
							batchScriptTextArea.requestFocus();
						}
					}
				}
			};

			// remove listeners
			for (MouseListener listener : tableColumnsList.getMouseListeners()) {
				if (listener.getClass().equals(tableColumnsListMouseAdapter.getClass())) {
					tableColumnsList.removeMouseListener(listener);					
				}				
			}
			tableColumnsList.addMouseListener(tableColumnsListMouseAdapter);
		}

		for (DialogComponent comp : getDialogComponents()) {
			comp.loadSettingsFrom(settings, specs);
		}
		batchScriptTextArea.setText(settings.getString(OrientDBExecuteNodeModel.CFGKEY_BATCH_SCRIPT, ""));
		returnConstructionTextField.setText(settings.getString(OrientDBExecuteNodeModel.CFGKEY_BATCH_RETURN, ""));
		generateByTemplateCheckBox
				.setSelected(settings.getBoolean(OrientDBExecuteNodeModel.CFGKEY_BATCH_GENERATE_BY_TEMPLATE, false));
		
		SpinnerNumberModel spinerModel = (SpinnerNumberModel) batchSizeSpinner.getModel();
		try {
			String strValue = settings.getString(OrientDBExecuteNodeModel.CFGKEY_BATCH_SIZE);
			logger.debug("====batch size " + strValue);
			spinerModel.setValue(Integer.parseInt(strValue));			
		} catch (InvalidSettingsException e1) {
			logger.warn("Can't read value for " + OrientDBExecuteNodeModel.CFGKEY_BATCH_SIZE, e1);
		}
	
	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) throws InvalidSettingsException {
		for (DialogComponent comp : getDialogComponents()) {
			comp.saveSettingsTo(settings);
		}
		settings.addString(OrientDBExecuteNodeModel.CFGKEY_BATCH_SCRIPT, batchScriptTextArea.getText());
		settings.addString(OrientDBExecuteNodeModel.CFGKEY_BATCH_RETURN, returnConstructionTextField.getText());
		
		settings.addString(CFGKEY_SELECTED_FLOW_VARIABLES,
				flowVariablesList.getSelectedValue());
		settings.addString(CFGKEY_SELECTED_SCHEMA_CLASSES, classesList.getSelectedValue());

		settings.addBoolean(OrientDBExecuteNodeModel.CFGKEY_BATCH_GENERATE_BY_TEMPLATE,
				generateByTemplateCheckBox.isSelected());
		SpinnerNumberModel spinerModel = (SpinnerNumberModel) batchSizeSpinner.getModel();
		int selectedBatchSize = spinerModel.getNumber().intValue();
		if (selectedBatchSize > OrientDBExecuteNodeModel.MAX_BATCH_SIZE) {
			logger.warn("Batch size exceeds " + OrientDBExecuteNodeModel.MAX_BATCH_SIZE + "!");
		} else if (selectedBatchSize == 0) {
			throw new InvalidSettingsException(
					"Batch size must not be between 1 and " + OrientDBExecuteNodeModel.MAX_BATCH_SIZE + "!");
		}
		settings.addString(OrientDBExecuteNodeModel.CFGKEY_BATCH_SIZE, spinerModel.getNumber().toString());

	}

	@Override
	protected OrientDBConnectionSettings getConnectionSettings() {
		return this.connectionSettings;
	}

	private static class ClassFilter implements Predicate<OClass> {
		private static final List<String> SYSTEM_NAMES = Arrays.asList("OSchedule", "OSequence", "OFunction");

		@Override
		public boolean test(OClass oClass) {
			boolean hasName = !oClass.getName().isEmpty();
			boolean realClass = !oClass.isAbstract();
			boolean isVertexOrEdgeOrDocument = (oClass.isSubClassOf("E") || oClass.isSubClassOf("V")
					|| oClass.getSuperClasses().isEmpty());
			boolean isNotSystemClass = !SYSTEM_NAMES.contains(oClass.getName());
			return hasName && realClass && isVertexOrEdgeOrDocument && isNotSystemClass;
		}

	}

}
