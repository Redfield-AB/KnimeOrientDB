package se.redfield.node.port.orientdb.function;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.json.JSONCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import se.redfield.node.port.orientdb.Constants;
import se.redfield.node.port.orientdb.Messages;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObject;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObjectSpec;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionSettings;
import se.redfield.node.port.orientdb.util.CredentionalUtil;

/**
 * This is the model implementation of OrientDBNodeTest.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBFunctionNodeModel extends NodeModel {

	// the logger instance
	public static final String CFGKEY_FUNCTION = "Function";
	public static final String CFGKEY_LOAD_DOC = "LOAD_DOC";
	public static final String CFGKEY_FUNCTION_PARAM_NAMES = "FUNCTION_PARAM_NAMES";

	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBFunctionNodeModel.class);

	private final SettingsModelString m_function = new SettingsModelString(CFGKEY_FUNCTION, null);
	private final SettingsModelStringArray m_function_param_names = new SettingsModelStringArray(CFGKEY_FUNCTION_PARAM_NAMES, null);
	private final SettingsModelBoolean m_load_documents = new SettingsModelBoolean(CFGKEY_LOAD_DOC, true);
	private final Map<String,String> m_function_param_values = new HashMap<>();
	
	
	private OrientDBConnectionSettings connectionSettings;

	static final int FLOW_VARIABLE_INDEX = 0;
	static final int DATA_TABLE_INDEX = 1;
	static final int ORIENTDB_CONNECTION_INDEX = 2;
	private DataTableSpec configuredTableSpec = null;
	private boolean existsInputTable = false;
	private boolean existsInputFlowVariables = false;

	/**
	 * Constructor for the node model.
	 */
	protected OrientDBFunctionNodeModel() {
		super(new PortType[] { FlowVariablePortObject.TYPE_OPTIONAL, BufferedDataTable.TYPE_OPTIONAL,
				OrientDBConnectionPortObject.TYPE },
				new PortType[] { BufferedDataTable.TYPE,OrientDBConnectionPortObject.TYPE  });
	}

	@Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs == null || inSpecs[ORIENTDB_CONNECTION_INDEX] == null) {
			throw new InvalidSettingsException("No required input available");
		}
		logger.info("==configure ==");		
		if (m_function.getStringValue() == null || m_function.getStringValue().isEmpty()) {
			 throw new InvalidSettingsException(Messages.NO_FUNCTION);
		}
		logger.info("function :" + m_function.getStringValue());
		existsInputFlowVariables =  (inSpecs[OrientDBFunctionNodeModel.FLOW_VARIABLE_INDEX] != null);
		existsInputTable = (inSpecs.length > OrientDBFunctionNodeModel.DATA_TABLE_INDEX
				&& inSpecs[OrientDBFunctionNodeModel.DATA_TABLE_INDEX] != null);
		
		if (existsInputFlowVariables && existsInputTable) {
			throw new InvalidSettingsException("Use flow variables or input table separately!");			
		} else if (!existsInputFlowVariables && !existsInputTable) {
			throw new InvalidSettingsException("Please make an input of Flow Variable or a table.");			
		}
		
		OrientDBConnectionPortObjectSpec orientDbSpec = (OrientDBConnectionPortObjectSpec) inSpecs[ORIENTDB_CONNECTION_INDEX];
		setConnectionSettings(orientDbSpec.getConnectionSettings(getCredentialsProvider()));
		PortObjectSpec[] result = null;
		
		if (inSpecs[DATA_TABLE_INDEX] != null) {
			logger.info("using data table as datasource");
			DataTableSpec dataTableSpec = (DataTableSpec) inSpecs[DATA_TABLE_INDEX];
			List<DataColumnSpec> structure = new LinkedList<>();
			for (String column : dataTableSpec.getColumnNames()) {
				structure.add(dataTableSpec.getColumnSpec(column));				
			}
			structure.add( new DataColumnSpecCreator("result", JSONCell.TYPE).createSpec());
			DataTableSpec newDataTableSpec = new DataTableSpec(structure.toArray(new DataColumnSpec[structure.size()]));
			logger.infoWithFormat("new column cell %s", structure.size());
			setConfiguredTableSpec(newDataTableSpec);
			result =  new PortObjectSpec[] { newDataTableSpec,orientDbSpec  };
		} else  {
			List<DataColumnSpec> structure = new LinkedList<>();			
			structure.add( new DataColumnSpecCreator("result", JSONCell.TYPE).createSpec());
			DataTableSpec newDataTableSpec = new DataTableSpec(structure.toArray(new DataColumnSpec[1]));
			logger.infoWithFormat("new column cell %s", structure.size());
			setConfiguredTableSpec(newDataTableSpec);
			result =  new PortObjectSpec[] { newDataTableSpec,orientDbSpec  };
			
		} 
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		logger.info("==execute ==");
		OrientDBConnectionPortObject orientDBConnectionPortObject = (OrientDBConnectionPortObject) inData[ORIENTDB_CONNECTION_INDEX];
		logger.infoWithFormat("orientDBConnectionPortObject: %s", orientDBConnectionPortObject);

		logger.infoWithFormat("!!!!!OrientDBConnectionSettings: %s", connectionSettings.getPoolSize());
		logger.info("Try to create connection...");
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(getConnectionSettings().getUserName(), getConnectionSettings().getPassword(),
				getConnectionSettings().getCredName(), getCredentialsProvider());

		OrientDB orientDBEnv = new OrientDB(getConnectionSettings().getDbUrl(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv,getConnectionSettings().getDbName(), userLogin.getLogin(),
				userLogin.getDecryptedPassword() );

		logger.info("function :" + m_function.getStringValue());
		String funcName = Utils.extractName(m_function.getStringValue()).toUpperCase(Locale.ENGLISH);
		List<String> funcParameters = Arrays.asList(m_function_param_names.getStringArrayValue());
		logger.infoWithFormat("name: %s ; parameters : %s", funcName, funcParameters);
		List<PortObject> returnResult = new LinkedList<>();
		StringBuilder commandBuilder = new StringBuilder();
		commandBuilder.append("SELECT ").append(funcName).append("(");
		commandBuilder.append(funcParameters.stream()
				.filter((String paramName)->{ return !paramName.trim().isEmpty();})
				.map( (String paramName)->{ return ":"+paramName;}).collect(Collectors.joining(",")) );
		commandBuilder.append(") as result");
		
		DataTableSpec dataTableSpec = getConfiguredTableSpec();
		BufferedDataContainer container = exec.createDataContainer(dataTableSpec);
		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			logger.info("Try to execute commands ...");

			if (inData[DATA_TABLE_INDEX] != null) {
				logger.debug("using input table for set values to function arguments");
				// use table as input
				BufferedDataTable dataTable = (BufferedDataTable) inData[DATA_TABLE_INDEX];
				
				AtomicLong rowCounter = new AtomicLong();
				
				List<Integer> paramColumnIndexes = indexColumns(dataTableSpec,funcParameters);
				
				try (CloseableRowIterator rowItertor = dataTable.iterator()) {
					while (rowItertor.hasNext()) {
						DataRow dataRow = rowItertor.next();
						List<Object> parameters = new LinkedList<>();
						List<DataCell> rowDataCells = new LinkedList<DataCell>();						
						rowDataCells.addAll( dataRow.stream().collect(Collectors.toList()));
						for (int index = 0; index < paramColumnIndexes.size(); index++) {
							int columnIndex = paramColumnIndexes.get(index);
							if (columnIndex > -1) {
								DataCell dataCell = dataRow.getCell(columnIndex);
								if (StringCell.TYPE.equals(dataCell.getType())) {
									StringCell strCell = (StringCell) dataCell;
									parameters.add(strCell.getStringValue());
								} else if (IntCell.TYPE.equals(dataCell.getType())) {
									IntCell strCell = (IntCell) dataCell;
									parameters.add(strCell.getIntValue());
								} else if (DoubleCell.TYPE.equals(dataCell.getType())) {
									DoubleCell strCell = (DoubleCell) dataCell;
									parameters.add(strCell.getDoubleValue());
								} else {
									throw new UnsupportedOperationException(
											"Unsupported cell type " + dataCell.getType() + " !");
								}
							} else {
								String paramName = funcParameters.get(index);
								logger.warnWithFormat("Column with name %s not found. Is it direct value? ",
										funcParameters.get(index));
								String directValue = m_function_param_values.get(paramName);
								parameters.add(directValue);

							}		
						}
						logger.infoWithFormat("Execute command: %s ; arguments: %s", commandBuilder,parameters);
						try (OResultSet resultSet = databaseSession.command(commandBuilder.toString(),
								parameters.toArray())) {
							if (resultSet.hasNext()) {
								OResult oResult = resultSet.next();
								logger.debugWithFormat("result : %s ", oResult.toJSON());
								Object resultObject = oResult.getProperty("result");
								if (isORecordIdCollection(resultObject) && m_load_documents.getBooleanValue()) {
									// it is result as collection of RID.
									StringBuilder jsonBuilder = new StringBuilder(1000);
									List<ORecordId> rids = (List<ORecordId>) resultObject;
									jsonBuilder.append("{ \"result\": [");
									List<ORecord> documents = getDocuments(databaseSession, rids);
									jsonBuilder.append(documents.stream().map((ORecord doc) -> {
										return doc.toJSON();
									}).collect(Collectors.joining(",")));
									jsonBuilder.append("]}");
									logger.info("JSON :"+jsonBuilder.toString());
									rowDataCells.add(Constants.JSON_CELL_FACTORY.createCell(jsonBuilder.toString()));
								} else {
									rowDataCells.add(Constants.JSON_CELL_FACTORY.createCell(oResult.toJSON()));
								}
								logger.debugWithFormat("columns : %s ", rowDataCells.size());
							} else {
								rowDataCells.add(Constants.JSON_CELL_FACTORY.createCell(
										"{\"result\":\"function '" + funcName + "' didn't return results \"}"));
							}
							DataRow newRow = new DefaultRow(dataRow.getKey(), rowDataCells);
							container.addRowToTable(newRow);
						}
						exec.checkCanceled();
						rowCounter.incrementAndGet();
						exec.setProgress(rowCounter.doubleValue() / dataTable.size(),
								"Executed " + rowCounter + " command");	
					}
				}
				
			} else  {
				logger.debug("using parameters for set values to function arguments");
				
				List parameters = new LinkedList();
				for (String paramName : m_function_param_names.getStringArrayValue()) {
					String flowVariableName = m_function_param_values.get(paramName);
					logger.debugWithFormat("paramName: %s ,flowVariableName: %s",paramName,flowVariableName);
					if (getAvailableFlowVariables().containsKey(flowVariableName)) {
						FlowVariable flowVariable = getAvailableFlowVariables().get(flowVariableName);
						if (flowVariable.getType().equals(Type.STRING)) {
							parameters.add(flowVariable.getStringValue());
						} else if (flowVariable.getType().equals(Type.INTEGER)) {
							parameters.add(flowVariable.getIntValue());
						} else if (flowVariable.getType().equals(Type.DOUBLE)) {
							parameters.add(flowVariable.getDoubleValue());
						}
					} else {
						logger.warnWithFormat("FlowVariable with name %s not found. Is it direct value? ",flowVariableName);
						parameters.add(flowVariableName);
					}				
				}
				List<DataCell> rowDataCells = new LinkedList<DataCell>();
				logger.infoWithFormat("Execute command: %s ; arguments: %s", commandBuilder,parameters);
				try (OResultSet resultSet = databaseSession.command(commandBuilder.toString(), parameters.toArray())) {
					if (resultSet.hasNext()) {
						OResult oResult = resultSet.next();
						logger.infoWithFormat("result : %s ", oResult.toJSON());
						Object resultObject = oResult.getProperty("result");
						logger.infoWithFormat("1. result class name : %s ", resultObject.getClass().getName());
						if (isORecordIdCollection(resultObject)  && m_load_documents.getBooleanValue()) {
							// it is result as collection of RID.
							StringBuilder jsonBuilder = new StringBuilder(1000);
							List<ORecordId> rids = (List<ORecordId>) resultObject;
							jsonBuilder.append("{ \"result\": [");
							List<ORecord> documents = getDocuments(databaseSession, rids);
							jsonBuilder.append(documents.stream().map((ORecord doc) -> {
								return doc.toJSON();
							}).collect(Collectors.joining(",")));
							jsonBuilder.append("]}");
							logger.info("JSON :"+jsonBuilder.toString());
							rowDataCells.add(Constants.JSON_CELL_FACTORY.createCell(jsonBuilder.toString()));
						} else {
							rowDataCells.add(Constants.JSON_CELL_FACTORY.createCell(oResult.toJSON()));
						}
						
						logger.debugWithFormat("columns : %s ", rowDataCells.size());
					} else {
						rowDataCells.add(Constants.JSON_CELL_FACTORY
								.createCell("{\"result\":\"function '" + funcName + "' didn't return results \"}"));
					}
					DataRow newRow = new DefaultRow("Row1", rowDataCells);
					container.addRowToTable(newRow);
				}					
			} 
			container.close();
			BufferedDataTable out = container.getTable();
			returnResult.add(out);
		} finally {
			orientDBPool.close();
			orientDBEnv.close();
		}
		returnResult.add(orientDBConnectionPortObject);
		return returnResult.toArray(new PortObject[2]);
	}
	
	private boolean isORecordIdCollection(Object resultObject) {
		return (resultObject instanceof List && !((List) resultObject).isEmpty() && ((List) resultObject).get(0) instanceof ORecordId);
	}
	
	private List<ORecord> getDocuments(ODatabaseSession databaseSession, List<ORecordId> rids) {
		List<ORecord> documents = new ArrayList<>(rids.size());
		StringBuilder builder = new StringBuilder();
		builder.append("select expand([");
		builder.append(rids.stream().map((ORecordId rid) -> {
			return rid.toString();
		}).collect(Collectors.joining(",")));
		builder.append("])");
		try (OResultSet resultSet = databaseSession.query(builder.toString())) {
			while (resultSet.hasNext()) {
				OResult oResult = resultSet.next();
				if (oResult.isRecord()) {
					documents.add((ORecord) oResult.getRecord().get());
				} else {
					logger.info("==!!!!==="+oResult.toJSON());
				}
			}
		}
		return documents;
	}

	private List<Integer> indexColumns(DataTableSpec dataTableSpec, List<String> funcParameters) {
		
		return funcParameters.stream().map((String paramName) -> {
			String targetColumnName = m_function_param_values.get(paramName);
			logger.infoWithFormat("paramName: %s,targetColumnName: %s", paramName,targetColumnName);
			return dataTableSpec.findColumnIndex(targetColumnName);
		}).collect(Collectors.toList());

	}
	
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// TODO Code executed on reset.
		// Models build during execute are cleared here.
		// Also data handled in load/saveInternals will be erased here.

	}

	

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		logger.info("== saveSettingsTo ===");
		this.m_function.saveSettingsTo(settings);
		this.m_function_param_names.saveSettingsTo(settings);
		this.m_load_documents.saveSettingsTo(settings);
		
		for (Map.Entry<String, String> entry :m_function_param_values.entrySet() ) {
			settings.addString(entry.getKey(), entry.getValue());
		}
		
		if (connectionSettings != null) {
			logger.info("== saveSettingsTo ===" + connectionSettings.getDbName());
			this.connectionSettings.saveConnection(settings);
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		logger.info("=!== loadValidatedSettingsFrom settings.keySet() :"+settings.keySet());
		logger.info("function :"+settings.getString(CFGKEY_FUNCTION, "==no value==="));
		this.m_function.loadSettingsFrom(settings);
		this.m_load_documents.loadSettingsFrom(settings);
		this.m_function_param_names.loadSettingsFrom(settings);
		logger.info("param names:"+Arrays.asList(m_function_param_names.getStringArrayValue()));
		for (String functionParamName : m_function_param_names.getStringArrayValue()) {
			this.m_function_param_values.put(functionParamName, settings.getString(functionParamName));
		}
		logger.info("param names:"+m_function_param_values);
		this.connectionSettings = new OrientDBConnectionSettings();
		try {
			this.connectionSettings.loadValidatedConnection(settings, getCredentialsProvider());
		} catch (InvalidSettingsException ise) {
			logger.error("Cannot load conneciton configuration!", ise);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		this.m_function.validateSettings(settings);
		this.m_load_documents.validateSettings(settings);
		
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		logger.info(" call loadInternals ");

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		logger.info(" call saveInternals ");
	}

	OrientDBConnectionSettings getConnectionSettings() {
		return connectionSettings;
	}

	private void setConnectionSettings(OrientDBConnectionSettings connectionSettings) {
		this.connectionSettings = connectionSettings;
		
	}

	private DataTableSpec getConfiguredTableSpec() {
		return configuredTableSpec;
	}

	private void setConfiguredTableSpec(DataTableSpec configuredTableSpec) {
		this.configuredTableSpec = configuredTableSpec;
	}
	

}
