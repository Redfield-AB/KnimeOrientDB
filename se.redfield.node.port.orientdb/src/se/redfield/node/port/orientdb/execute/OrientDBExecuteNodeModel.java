package se.redfield.node.port.orientdb.execute;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.collection.SetCell;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.RowAppender;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
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
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import se.redfield.node.port.orientdb.Constants;
import se.redfield.node.port.orientdb.command.DataTypeUtil;
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
public class OrientDBExecuteNodeModel extends NodeModel implements FlowVariableProvider {

	// the logger instance
	public static final String CFGKEY_BATCH_SCRIPT = "Batch script";
	public static final String CFGKEY_BATCH_GENERATE_BY_TEMPLATE = "Generate by template";
	public static final String CFGKEY_BATCH_SIZE = "BATCH_SIZE";
	public static final String CFGKEY_BATCH_RETURN = "BATCH_RETURN";
	public static final int MAX_BATCH_SIZE = 5_000;

	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBExecuteNodeModel.class);

	private final SettingsModelString m_command = new SettingsModelString(CFGKEY_BATCH_SCRIPT, null);
	private final SettingsModelString m_return = new SettingsModelString(CFGKEY_BATCH_RETURN, null);
	private final SettingsModelBoolean m_generate_by_template = new SettingsModelBoolean(
			CFGKEY_BATCH_GENERATE_BY_TEMPLATE, false);
	private final SettingsModelString m_batch_size = new SettingsModelString(CFGKEY_BATCH_SIZE, String.valueOf(MAX_BATCH_SIZE));

	static final int ORIENTDB_CONNECTION_INDEX = 2;
	static final int DATA_TABLE_INDEX = 1;
	
	private Pattern COLUMN_NAME_PATTER = Pattern.compile("(\\$\\$\\([\\d\\w]+\\)\\$\\$),*");

	private DataTableSpec configuredTableSpec;
	private OrientDBConnectionSettings connectionSettings;
	private boolean hasInputTable = false;

	/**
	 * Constructor for the node model.
	 */
	protected OrientDBExecuteNodeModel() {
		super(new PortType[] { FlowVariablePortObject.TYPE_OPTIONAL, BufferedDataTable.TYPE_OPTIONAL,
				OrientDBConnectionPortObject.TYPE },
				new PortType[] { FlowVariablePortObject.TYPE_OPTIONAL, BufferedDataTable.TYPE_OPTIONAL,OrientDBConnectionPortObject.TYPE  });
	}

	@Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs == null || inSpecs[ORIENTDB_CONNECTION_INDEX] == null) {
			throw new InvalidSettingsException("No required input available");
		}
		DataTableSpec dataTableSpec = new DataTableSpec(
				new DataColumnSpecCreator("result", JSONCell.TYPE).createSpec());
		setConfiguredTableSpec(dataTableSpec);
		OrientDBConnectionPortObjectSpec spec = (OrientDBConnectionPortObjectSpec) inSpecs[ORIENTDB_CONNECTION_INDEX];

		setConnectionSettings(spec.getConnectionSettings(getCredentialsProvider()));

		hasInputTable = (inSpecs[DATA_TABLE_INDEX] != null);
		if (hasInputTable && !m_generate_by_template.getBooleanValue()) {
			throw new InvalidSettingsException("You need use templates if you use input data table!");
		}
		return new PortObjectSpec[] { FlowVariablePortObjectSpec.INSTANCE, dataTableSpec,spec };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		OrientDBConnectionPortObject orientDBConnectionPortObject = (OrientDBConnectionPortObject) inData[ORIENTDB_CONNECTION_INDEX];

		OrientDBConnectionSettings connectionSettings = orientDBConnectionPortObject
				.getConnectionSettings(getCredentialsProvider());

		logger.info("Try to create connection...");
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(getConnectionSettings().getUserName(), getConnectionSettings().getPassword(),
				getConnectionSettings().getCredName(), getCredentialsProvider());
		OrientDB orientDBEnv = new OrientDB(connectionSettings.getDbUrl(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv, connectionSettings.getDbName(),
				userLogin.getLogin(),
				userLogin.getDecryptedPassword());
		DataTableSpec dataTableSpec = getConfiguredTableSpec();
		BufferedDataContainer container = exec.createDataContainer(dataTableSpec);
		
		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			logger.info("Try to execute commands ...");
			if (inData[DATA_TABLE_INDEX] != null) {
				logger.debug("using input table for set values to function arguments");
				BufferedDataTable dataTable = (BufferedDataTable) inData[DATA_TABLE_INDEX];
				int batchIndex = 1;
				for (String batchScript : generateBatchScript(dataTable)) {
					logger.info("start execute batch N"+batchIndex);
					executeBatchScript(databaseSession, batchScript,container, batchIndex++);
					logger.info("finish execute batch N"+batchIndex);
				}
			} else {
				String batchScript = FlowVariableResolver.parse(m_command.getStringValue(), this);
				logger.infoWithFormat("Execute batch: %s", batchScript);
				logger.info("start execute batch N1");
				executeBatchScript(databaseSession, batchScript,  container, 1);
				logger.info("finish execute batch N 1");
			}

		} finally {
			orientDBPool.close();
			orientDBEnv.close();
		}
		container.close();
		BufferedDataTable out = container.getTable();
		return new PortObject[] { FlowVariablePortObject.INSTANCE, out, orientDBConnectionPortObject };
	}
	
	void executeBatchScript(ODatabaseSession databaseSession, String  batchScript,RowAppender rowAppender, int batchIndex) {
		try (OResultSet resultSet = databaseSession.execute("sql", batchScript)) {
			if (resultSet.hasNext()) {
				OResult oResult = resultSet.next();
				pushFlowVariableString("result"+batchIndex, oResult.toJSON());
				DataRow newRow = new DefaultRow("result"+batchIndex, Constants.JSON_CELL_FACTORY.createCell(oResult.toJSON()));
				rowAppender.addRowToTable(newRow);
			} else {
				DataRow newRow = new DefaultRow("result"+batchIndex, Constants.JSON_CELL_FACTORY.createCell("{\"result\":\"script didn't return result\"}"));
				rowAppender.addRowToTable(newRow);
			}
		}
	}
	
	private String prepareBatchRow(String template,DataRow dataRow,BufferedDataTable dataTable) {
		String batchRow = template;
		boolean findGroup = false;
		do {
			Matcher matcher = COLUMN_NAME_PATTER.matcher(batchRow);
			findGroup = matcher.find();
			logger.info("findGroup:"+ findGroup);
			if (findGroup) {
				logger.info("matcher.groupCount: "+ matcher.groupCount());
				String group = matcher.group(0);
				logger.info("group: "+ group);
				String columnName = getColumnName(group);
				logger.info("columnName:"+ columnName);
				DataCell dataCell = dataRow.getCell(dataTable.getDataTableSpec().findColumnIndex(columnName));
				String value = "null";
				if (dataCell != null && !dataCell.isMissing()) {
					value = getStringValue(dataCell);
				}
				if (group.endsWith(",")) {
					value = value+",";					
				}
				batchRow = batchRow.replace(group, value);
			}
		} while (findGroup);
		return batchRow;
	}
	
	private String getStringValue(DataCell dataCell) {		
		String value = "null";
		if (dataCell.getType().getCellClass().equals(ListCell.class)) {
			// it is list
			logger.info("it is list");
			ListCell cell = (ListCell) dataCell;
			value = "[" + cell.stream().map((DataCell dc) -> {
				return DataTypeUtil.getDataCellValue(dc);
			}).map(new StringValuePreparer()).collect(Collectors.joining(",")) + "]";
			return value;
		} else if (dataCell.getType().getCellClass().equals(SetCell.class))  {
			logger.info("it is set");
			SetCell cell = (SetCell) dataCell;
			value = "[" + cell.stream().map((DataCell dc) -> {
				return DataTypeUtil.getDataCellValue(dc);
			}).map(new StringValuePreparer()).collect(Collectors.joining(",")) + "]";
			return value;
		} 
		Object objValue = DataTypeUtil.getDataCellValue(dataCell);
		if (objValue == null) {
			throw new UnsupportedOperationException("Unsupported cell type " + dataCell.getType() + " !");
		} else if (objValue instanceof String) {
			value = (String) objValue;
		} else {
			value = objValue.toString();
		}		
		return value;
	}

	private Collection<String> generateBatchScript(BufferedDataTable dataTable) {
		List<String> batches = new LinkedList<String>();
		final int initialLength = ("BEGIN;"+System.lineSeparator()).length();
		// we have a template and ... we must generate batch script that insert all rows
		// from table
		String template = FlowVariableResolver.parse(m_command.getStringValue(), this);
		StringBuilder batchScriptBuffer = new StringBuilder(10_000);
		batchScriptBuffer.append("BEGIN;").append(System.lineSeparator());
		int batchSizeLimit = Integer.parseInt(m_batch_size.getStringValue());
		logger.infoWithFormat("batch size limit: %s", batchSizeLimit);
		int rowCount = 0;
		try (CloseableRowIterator rowItertor = dataTable.iterator()) {
			while (rowItertor.hasNext()) {
				DataRow dataRow = rowItertor.next();
				String batchRow =prepareBatchRow(template,dataRow,dataTable);
				batchScriptBuffer.append(batchRow).append(";").append(System.lineSeparator());
				rowCount++;
				if (rowCount == batchSizeLimit) {
					setBatchScriptFooter(batchScriptBuffer, rowCount);
					batches.add(batchScriptBuffer.toString());
					batchScriptBuffer.setLength(initialLength);
					rowCount = 0;
				}
			}
			if (batchScriptBuffer.length() > initialLength) {
				setBatchScriptFooter(batchScriptBuffer, rowCount);
				batches.add(batchScriptBuffer.toString());
				batchScriptBuffer.setLength(0); // clear buffer
			}
		}
		logger.infoWithFormat("=====batches: %s", batches);
		
		return batches;
	}

	private void setBatchScriptFooter(StringBuilder batchScriptBuffer, int rowCount) {
		batchScriptBuffer.append("COMMIT;").append(System.lineSeparator());
		if (m_return.getStringValue() != null && !m_return.getStringValue().isEmpty()) {
			batchScriptBuffer.append(m_return.getStringValue()).append(";").append(System.lineSeparator());
		} else {
			batchScriptBuffer.append("RETURN ").append(rowCount).append(";").append(System.lineSeparator());
		}
//		logger.info("batch script :" + batchScriptBuffer.toString());
	}
	
	

	private String getColumnName(String group) {
		int startPos = group.trim().indexOf("$$(");
		int endPos = group.trim().lastIndexOf(")$$");
		return group.substring(startPos+3, endPos);
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
		m_command.saveSettingsTo(settings);
		m_generate_by_template.saveSettingsTo(settings);
		m_batch_size.saveSettingsTo(settings);
		m_return.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_command.loadSettingsFrom(settings);
		m_generate_by_template.loadSettingsFrom(settings);
		m_batch_size.loadSettingsFrom(settings);
		m_return.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_command.validateSettings(settings);
		m_generate_by_template.validateSettings(settings);
		m_batch_size.validateSettings(settings);
		m_return.validateSettings(settings);
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

	private DataTableSpec getConfiguredTableSpec() {
		return configuredTableSpec;
	}

	private void setConfiguredTableSpec(DataTableSpec configuredTableSpec) {
		this.configuredTableSpec = configuredTableSpec;
	}

	boolean hasInputTable() {
		return this.hasInputTable;
	}

	protected OrientDBConnectionSettings getConnectionSettings() {
		return connectionSettings;
	}

	protected void setConnectionSettings(OrientDBConnectionSettings connectionSettings) {
		this.connectionSettings = connectionSettings;
	}

}
