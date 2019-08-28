package se.redfield.node.port.orientdb.query;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.LayoutStyle;
import javax.swing.SwingConstants;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
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
public class OrientDBQueryNodeDialog extends AbstractOrientDBNodeDialogPane {
	private static final String TABLE_QUERY_SETTINGS = "Table Query Settings";
	private static final String QUERY_SETTINGS = "Query Settings";
	
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBQueryNodeDialog.class);
	private static final ClassFilter CLASS_FILTER = new ClassFilter();
	public static final String DEFAUT_SCHEMA_SOURCE = "_dynamic_";
	public static final String TO_JSON_SCHEMA_SOURCE = "_as_json_";
	public static final String CFGKEY_SELECTED_FLOW_VARIABLES = "SELECTED_FLOW_VARIABLES";
	public static final String CFGKEY_SELECTED_SCHEMA_CLASSES = "SELECTED_CLASSES";
	public static final String CFGKEY_SCHEMA_SOURCE = "SCHEMA_SOURCE";
	public static final String CFGKEY_QUERY_FIELD = "QUERY_FIELD";

	private DefaultListModel<String> flowVariablesListModel= new DefaultListModel<>();;
	
	private DefaultListModel<String> classesListModel= new DefaultListModel<>();
	private DefaultListModel<String> fieldsListModel= new DefaultListModel<>();
	private DefaultComboBoxModel<String> schemaListModel = new DefaultComboBoxModel<>();

	private javax.swing.JList<String> classesList;
	private javax.swing.JList<String> fieldsList;
	private javax.swing.JList<String> flowVariablesList;
	private javax.swing.JSplitPane fvClassSplitPane;
	private javax.swing.JLabel takeSchemaLabel;
	private javax.swing.JScrollPane schemaScrollPane;
	private javax.swing.JTextPane queryTextPane;
	private javax.swing.JComboBox<String> schemaComboBox;
	
	private javax.swing.JComboBox<String>  fieldWithQuery;
	private DefaultComboBoxModel<String> fieldWithQueryListModel = new DefaultComboBoxModel<>();
	private javax.swing.JCheckBox useParallelExecution = new JCheckBox("Use parallel execution", true);
	

	private OrientDBConnectionSettings connectionSettings;
	private JCheckBox generateRowIdCheckBox;
	private JCheckBox loadAsJson;

	protected OrientDBQueryNodeDialog() {
		super();

		initQuerySettingsComponents();
		initTableQuerySettingsComponents();

		// add listeners
		flowVariablesList.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent evt) {
				if (evt.getClickCount() == 2) {
					Object o = flowVariablesList.getSelectedValue();
					if (o != null) {
						String flowVariableName = (String) o;
						logger.infoWithFormat("mouseClicked:  flowVariableName: %s", flowVariableName);
						FlowVariable var = getAvailableFlowVariables().get(flowVariableName);
						queryTextPane.replaceSelection(FlowVariableResolver.getPlaceHolderForVariable(var));
						flowVariablesList.clearSelection();
						queryTextPane.requestFocus();
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
						queryTextPane.replaceSelection(fieldName);
						fieldsList.clearSelection();
						queryTextPane.requestFocus();
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
						queryTextPane.replaceSelection(className);
						queryTextPane.requestFocus();
					}
				}
			}
		});
		
		

	}
	 private void initQuerySettingsComponents() {
			JPanel settingPanel = new JPanel();
			GroupLayout settingPanetLayout = new GroupLayout(settingPanel);
			settingPanel.setLayout(settingPanetLayout);
			settingPanel.setPreferredSize(new Dimension(938, 414));

	        javax.swing.JSplitPane centralSplitPane = new javax.swing.JSplitPane();
	        fvClassSplitPane = new javax.swing.JSplitPane();
	        javax.swing.JScrollPane fvScrollPane = new javax.swing.JScrollPane();
	        flowVariablesList = new javax.swing.JList<>();
	        javax.swing.JSplitPane classesFieldsSplitPane = new javax.swing.JSplitPane();
	        javax.swing.JScrollPane classesScrollPane = new javax.swing.JScrollPane();
	        classesList = new javax.swing.JList<>();
	        javax.swing.JScrollPane fieldsScrollPane = new javax.swing.JScrollPane();
	        fieldsList = new javax.swing.JList<>();
	        javax.swing.JSplitPane queryShemaSplitPane = new javax.swing.JSplitPane();
	        javax.swing.JScrollPane queryScrollPane = new javax.swing.JScrollPane();
	        javax.swing.JPanel queryPanel = new javax.swing.JPanel();
	        generateRowIdCheckBox = new JCheckBox();
	        loadAsJson = new JCheckBox();
	        loadAsJson.setSelected(true);
	        queryTextPane = new javax.swing.JTextPane();
	        schemaScrollPane = new javax.swing.JScrollPane();
	        javax.swing.JPanel takeSchemaPanel = new javax.swing.JPanel();
	        takeSchemaPanel.setPreferredSize(new Dimension(500,25));
	        takeSchemaPanel.setSize(new Dimension(500,25));
	        takeSchemaLabel = new javax.swing.JLabel();
	        schemaComboBox = new javax.swing.JComboBox<>();
	               

	        centralSplitPane.setDividerLocation(280);

	        fvClassSplitPane.setDividerLocation(195);
	        fvClassSplitPane.setOrientation(javax.swing.JSplitPane.VERTICAL_SPLIT);

	        fvScrollPane.setBorder(javax.swing.BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Flow Variables"));

	        flowVariablesList.setModel(flowVariablesListModel);
	        flowVariablesList.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
	        fvScrollPane.setViewportView(flowVariablesList);

	        fvClassSplitPane.setTopComponent(fvScrollPane);

	        classesScrollPane.setBorder(javax.swing.BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Classes"));

	        classesList.setModel(classesListModel);
	        classesScrollPane.setViewportView(classesList);

	        classesFieldsSplitPane.setLeftComponent(classesScrollPane);

	        fieldsScrollPane.setBorder(javax.swing.BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Fields"));

	        fieldsList.setModel(fieldsListModel);
	        fieldsScrollPane.setViewportView(fieldsList);

	        classesFieldsSplitPane.setRightComponent(fieldsScrollPane);
	        classesFieldsSplitPane.setDividerLocation(140);
	        fvClassSplitPane.setRightComponent(classesFieldsSplitPane);
	        centralSplitPane.setLeftComponent(fvClassSplitPane);

	        queryShemaSplitPane.setDividerLocation(50);
	        queryShemaSplitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);

	        queryScrollPane.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Query"));

	        generateRowIdCheckBox.setText("Generate RowId by @rid?");
	        loadAsJson.setText("Load collection fields as Json?");

	        javax.swing.GroupLayout queryPanelLayout = new javax.swing.GroupLayout(queryPanel);
	        queryPanel.setLayout(queryPanelLayout);
	        queryPanelLayout.setHorizontalGroup(
		            queryPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
		            .addGroup(queryPanelLayout.createSequentialGroup()
		                .addComponent(generateRowIdCheckBox, GroupLayout.PREFERRED_SIZE, 302, GroupLayout.PREFERRED_SIZE)
		                .addGap(0, 20, Short.MAX_VALUE)
		                .addComponent(loadAsJson, GroupLayout.PREFERRED_SIZE, 302, GroupLayout.PREFERRED_SIZE)
//		                .addGap(0, 20, Short.MAX_VALUE)
		                )
		            .addGroup(queryPanelLayout.createSequentialGroup()
		                .addContainerGap()
		                .addComponent(queryTextPane)
		                .addContainerGap())
		        );
		        queryPanelLayout.setVerticalGroup(
		            queryPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
		            .addGroup(queryPanelLayout.createSequentialGroup()
		            		.addGroup(queryPanelLayout.createParallelGroup()
		            				.addComponent(generateRowIdCheckBox)
		            				.addComponent(loadAsJson)
//		            				.addGap(0, 5, Short.MAX_VALUE)
		            				)
		                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
		                .addComponent(queryTextPane, GroupLayout.DEFAULT_SIZE, 273, Short.MAX_VALUE)
		                .addContainerGap())
		        );

	        queryScrollPane.setViewportView(queryPanel);

	        queryShemaSplitPane.setBottomComponent(queryScrollPane);

	        schemaScrollPane.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.black), "Schema"));

	        takeSchemaLabel.setHorizontalAlignment(SwingConstants.RIGHT);
	        takeSchemaLabel.setText("Take schema from :");

	        schemaComboBox.setModel(schemaListModel);

	        javax.swing.GroupLayout takeSchemaPanelLayout = new javax.swing.GroupLayout(takeSchemaPanel);
	        takeSchemaPanel.setLayout(takeSchemaPanelLayout);
	        takeSchemaPanelLayout.setHorizontalGroup(
	            takeSchemaPanelLayout.createParallelGroup(Alignment.LEADING)
	            .addGroup(takeSchemaPanelLayout.createSequentialGroup()
	            	.addGap(1)
	                .addComponent(takeSchemaLabel,GroupLayout.PREFERRED_SIZE, 150, GroupLayout.PREFERRED_SIZE)
	                .addGap(5)
	                .addComponent(schemaComboBox, GroupLayout.PREFERRED_SIZE, 150, GroupLayout.PREFERRED_SIZE)
	                .addGap(10)
	                .addContainerGap(300, Short.MAX_VALUE))
	        );
	        takeSchemaPanelLayout.setVerticalGroup(
	            takeSchemaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
	            .addGroup(takeSchemaPanelLayout.createSequentialGroup()
	                .addGap(1)
	                .addGroup(takeSchemaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
	                		.addComponent(takeSchemaLabel, 20, 20, 20)
		                    .addComponent(schemaComboBox, 20, 20, 20))
	                .addGap(1))
	        );

	        schemaScrollPane.setViewportView(takeSchemaPanel);

	        queryShemaSplitPane.setLeftComponent(schemaScrollPane);

	        centralSplitPane.setRightComponent(queryShemaSplitPane);

	        
	        settingPanetLayout.setHorizontalGroup(
	        		settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
	            .addComponent(centralSplitPane, javax.swing.GroupLayout.Alignment.TRAILING)
	        );
	        settingPanetLayout.setVerticalGroup(
	        		settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
	            .addGroup(settingPanetLayout.createSequentialGroup()
	                .addContainerGap()
	                .addComponent(centralSplitPane, javax.swing.GroupLayout.DEFAULT_SIZE, 365, Short.MAX_VALUE)
	                .addContainerGap())
	        );
	        super.addTab(QUERY_SETTINGS, settingPanel, true);
	    }
	 
	private void initTableQuerySettingsComponents() {
		JPanel settingPanel = new JPanel();
		GroupLayout settingPanetLayout = new GroupLayout(settingPanel);
		settingPanel.setLayout(settingPanetLayout);
		settingPanel.setPreferredSize(new Dimension(938, 414));
		
		fieldWithQuery = new JComboBox<String>();
		fieldWithQuery.setModel(fieldWithQueryListModel);
		javax.swing.JLabel jLabel1 = new  javax.swing.JLabel("Column with query");
		javax.swing.JLabel jLabel2 = new  javax.swing.JLabel("Execution type");
		
		
		settingPanetLayout.setHorizontalGroup(
				settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(settingPanetLayout.createSequentialGroup()
                .addGap(27, 27, 27)
                .addGroup(settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jLabel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(jLabel1, javax.swing.GroupLayout.DEFAULT_SIZE, 127, Short.MAX_VALUE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(fieldWithQuery, javax.swing.GroupLayout.PREFERRED_SIZE, 297, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(useParallelExecution, javax.swing.GroupLayout.PREFERRED_SIZE, 193, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(327, Short.MAX_VALUE))
        );
		settingPanetLayout.setVerticalGroup(
				settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(settingPanetLayout.createSequentialGroup()
                .addGap(22, 22, 22)
                .addGroup(settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel1)
                    .addComponent(fieldWithQuery, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(settingPanetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel2)
                    .addComponent(useParallelExecution))
                .addContainerGap(339, Short.MAX_VALUE))
        );
		super.addTab(TABLE_QUERY_SETTINGS, settingPanel, true);
	}
	

	

	@Override
	protected void loadSettingsFrom(NodeSettingsRO settings, PortObjectSpec[] specs) throws NotConfigurableException {
		logger.info("=!== loadSettingsFrom (PortObjectSpec) ====");
		if (specs == null || specs[OrientDBQueryNodeModel.ORIENTDB_CONNECTION_INDEX] == null) {
			throw new NotConfigurableException("No required input available");
		}
		OrientDBConnectionPortObjectSpec spec = (OrientDBConnectionPortObjectSpec) specs[OrientDBQueryNodeModel.ORIENTDB_CONNECTION_INDEX];
		try {
			this.connectionSettings = spec.getConnectionSettings(getCredentialsProvider());
		} catch (InvalidSettingsException e) {
			throw new NotConfigurableException("Connection settings haven't given", e);
		}
		logger.info(" (connectionSettings!=null) :" + (connectionSettings != null));

		flowVariablesListModel.removeAllElements();
		for (Map.Entry<String, FlowVariable> e : getAvailableFlowVariables().entrySet()) {
			flowVariablesListModel.addElement(e.getKey());
		}
		schemaListModel.removeAllElements();
		schemaListModel.addElement(DEFAUT_SCHEMA_SOURCE);
		schemaListModel.addElement(TO_JSON_SCHEMA_SOURCE);
		classesListModel.removeAllElements();
		Map<String, List<OProperty>> classesWithProperties;
		try {
			classesWithProperties = getAvailableClassesWithFields(CLASS_FILTER);
		} catch (InvalidSettingsException e2) {
			logger.error("Cannot get available classes from database!", e2);
			throw new NotConfigurableException("Cannot get available classes from database!", e2);
		}
		
		for (Map.Entry<String, List<OProperty>> entry : classesWithProperties.entrySet()) {
			classesListModel.addElement(entry.getKey());
			schemaListModel.addElement(entry.getKey());
		}

		try {
			classesList.setSelectedValue(settings.getString(CFGKEY_SELECTED_SCHEMA_CLASSES), true);
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
		queryTextPane.setText(settings.getString(OrientDBQueryNodeModel.CFGKEY_QUERY, null));
		generateRowIdCheckBox
				.setSelected(settings.getBoolean(OrientDBQueryNodeModel.CFGKEY_GENERATE_ROWID_BY_RID, false));
		String selectedSchemaSource = settings.getString(CFGKEY_SCHEMA_SOURCE, DEFAUT_SCHEMA_SOURCE);
		schemaComboBox.setSelectedItem(selectedSchemaSource);
		
		boolean useQueryFromTableColumn = (specs[OrientDBQueryNodeModel.DATA_TABLE_INDEX] != null);
		logger.infoWithFormat("useQueryFromTableColumn: %s", useQueryFromTableColumn);
		setEnabled(useQueryFromTableColumn, TABLE_QUERY_SETTINGS);
		setEnabled(!useQueryFromTableColumn, QUERY_SETTINGS);
		setSelected(useQueryFromTableColumn ? TABLE_QUERY_SETTINGS :  QUERY_SETTINGS);
		if (useQueryFromTableColumn) {
			fieldWithQueryListModel.removeAllElements();
			DataTableSpec tableTableSpec = (DataTableSpec) specs[OrientDBQueryNodeModel.DATA_TABLE_INDEX];
			for (String columnName :tableTableSpec.getColumnNames()) {
				fieldWithQueryListModel.addElement(columnName);
			}	
			fieldWithQuery.setSelectedItem(settings.getString(CFGKEY_QUERY_FIELD,null));
		}
	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) throws InvalidSettingsException {
		settings.addString(OrientDBQueryNodeModel.CFGKEY_QUERY, queryTextPane.getText());
		settings.addBoolean(OrientDBQueryNodeModel.CFGKEY_GENERATE_ROWID_BY_RID, generateRowIdCheckBox.isSelected());
		settings.addBoolean(OrientDBQueryNodeModel.CFGKEY_LOAD_AS_JSON, loadAsJson.isSelected());
		settings.addBoolean(OrientDBQueryNodeModel.CFGKEY_USE_PARALLEL, useParallelExecution.isSelected());
		
		settings.addString(CFGKEY_SELECTED_FLOW_VARIABLES, flowVariablesList.getSelectedValue());
		settings.addString(CFGKEY_SELECTED_SCHEMA_CLASSES, classesList.getSelectedValue());
		settings.addString(CFGKEY_SCHEMA_SOURCE, schemaComboBox.getSelectedItem().toString());
		try {
			settings.addString(CFGKEY_QUERY_FIELD, fieldWithQuery.getSelectedItem().toString());
		} catch (NullPointerException npe) {
			logger.warn("Cannot save property " + CFGKEY_QUERY_FIELD);
		}
	}

	@Override
	protected OrientDBConnectionSettings getConnectionSettings() {
		return connectionSettings;
	}

	// @todo refactor it
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
