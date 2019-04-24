package se.redfield.node.port.orientdb.command;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.date.DateAndTimeCellFactory;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.json.JSONCell;
import org.knime.core.data.time.localdate.LocalDateCell;
import org.knime.core.data.time.localdatetime.LocalDateTimeCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCell;
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
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelOptionalString;
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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

import se.redfield.node.port.orientdb.Constants;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObject;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObjectSpec;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionSettings;
import se.redfield.node.port.orientdb.util.CredentionalUtil;
import se.redfield.node.port.orientdb.util.DateTimeUtils;
import se.redfield.node.port.orientdb.util.FutureUtil;
import se.redfield.node.port.orientdb.util.OrientDbUtil;


/**
 * This is the model implementation of OrientDBNodeTest.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBCommandNodeModel extends NodeModel implements FlowVariableProvider {
    
	private static final String COMMAND_RESULT_COLUMN = "command_result";
	public static final String CFGKEY_COMMAND = "Command";
	public static final String CFGKEY_CLASS = "Class";
	public static final String CFGKEY_UPSERT = "Use upsert";
	public static final String CFGKEY_COLUMN_WITH_COMMAND = "Column with command";
	public static final String CFGKEY_MODE = "Mode";
	public static final String CFGKEY_EXECUTION = "Execution";
	public static final String CFGKEY_FIELDS = "Fields";
	
	public static final String WRITE_TABLE_FOR_CLASS = "Write table to class";
	public static final String USE_DIRECT_COMMAND = "Use SQL statement";
	public static final String USE_COMMAND_FROM_TABLE = "Use column with command";
	public static final String USE_PARALLEL_EXECUTION = "Use parallel execution";
	public static final String USE_SEQUENT_EXECUTION = "Use sequent execution";
	
	
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBCommandNodeModel.class);
    
    private OrientDBConnectionSettings connectionSettings;
    private DataTableSpec configuredTableSpec;
    private final SettingsModelString m_mode = new SettingsModelString(CFGKEY_MODE, USE_DIRECT_COMMAND);
    private final SettingsModelString m_execution = new SettingsModelString(CFGKEY_EXECUTION, USE_PARALLEL_EXECUTION);
    private final SettingsModelString m_command = new SettingsModelString(CFGKEY_COMMAND, null);
    private final SettingsModelString m_class = new SettingsModelString(CFGKEY_CLASS, null);
    private final SettingsModelFilterString m_fields = new SettingsModelFilterString(CFGKEY_FIELDS);
    private final SettingsModelOptionalString m_column_with_command = new SettingsModelOptionalString(CFGKEY_COLUMN_WITH_COMMAND, null,false);
    private final SettingsModelBoolean m_use_upsert = new SettingsModelBoolean(CFGKEY_UPSERT,false);
    
    static final int DATA_TABLE_INDEX = 0;
    static final int ORIENTDB_CONNECTION_INDEX = 1;
	
    
    
    
    /**
     * Constructor for the node model.
     */
    protected OrientDBCommandNodeModel() {    
    	super( new PortType[]{BufferedDataTable.TYPE_OPTIONAL,OrientDBConnectionPortObject.TYPE}, 
    			new PortType[] {FlowVariablePortObject.TYPE_OPTIONAL,BufferedDataTable.TYPE_OPTIONAL,OrientDBConnectionPortObject.TYPE});
    }
    
	@Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs == null || inSpecs[ORIENTDB_CONNECTION_INDEX] == null) {
			throw new InvalidSettingsException("No required input available");
		}		
		DataTableSpec tableTableSpec = (DataTableSpec) inSpecs[DATA_TABLE_INDEX];
		boolean useSingleCommand = (m_mode.isEnabled() && m_mode.getStringValue().equals(USE_DIRECT_COMMAND));
		
		if (useSingleCommand) {
			DataTableSpec dataTableSpec = new DataTableSpec( new DataColumnSpecCreator(COMMAND_RESULT_COLUMN, JSONCell.TYPE).createSpec());
			setConfiguredTableSpec(dataTableSpec);
		} else {
			List<DataColumnSpec> currentCollumns =  tableTableSpec.stream().collect(Collectors.toList());
			currentCollumns.add(new DataColumnSpecCreator(COMMAND_RESULT_COLUMN, JSONCell.TYPE).createSpec());
			setConfiguredTableSpec(new DataTableSpec(currentCollumns.toArray(new DataColumnSpec[currentCollumns.size()])));
		}
		
		OrientDBConnectionPortObjectSpec orientDbSpec = (OrientDBConnectionPortObjectSpec) inSpecs[ORIENTDB_CONNECTION_INDEX];
		
		setConnectionSettings(orientDbSpec.getConnectionSettings(getCredentialsProvider()));
		
		return new PortObjectSpec[] {FlowVariablePortObjectSpec.INSTANCE, getConfiguredTableSpec(),orientDbSpec  };
	}
	

	/**
     * {@inheritDoc}
     */
    @Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		logger.info("==execute ==");
		OrientDBConnectionPortObject orientDBConnectionPortObject = (OrientDBConnectionPortObject) inData[ORIENTDB_CONNECTION_INDEX];
		logger.infoWithFormat("orientDBConnectionPortObject: %s", orientDBConnectionPortObject);
		setConnectionSettings(orientDBConnectionPortObject.getConnectionSettings(getCredentialsProvider()));

		logger.info("Try to create connection...");		
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(getConnectionSettings().getUserName(), getConnectionSettings().getPassword(),
				getConnectionSettings().getCredName(), getCredentialsProvider());
		OrientDB orientDBEnv = new OrientDB(getConnectionSettings().getDbUrl(), userLogin.getLogin(),
				userLogin.getDecryptedPassword(), OrientDBConfig.defaultConfig());
		ODatabasePool orientDBPool = new ODatabasePool(orientDBEnv, getConnectionSettings().getDbName(),
				userLogin.getLogin(),
				userLogin.getDecryptedPassword());
		
		boolean useInputDataTable = (m_mode.isEnabled() && m_mode.getStringValue().equals(WRITE_TABLE_FOR_CLASS));
		boolean useSingleCommand = (m_mode.isEnabled() && m_mode.getStringValue().equals(USE_DIRECT_COMMAND));
		boolean useColumnWithCommand = (m_mode.isEnabled() && m_mode.getStringValue().equals(USE_COMMAND_FROM_TABLE));

		DataTableSpec dataTableSpec = getConfiguredTableSpec();
		BufferedDataContainer container = exec.createDataContainer(dataTableSpec);

		logger.info("Try to execute commands ...");
		if (useSingleCommand) {
			executeSingleCommand(orientDBPool, container);
		} else if (useInputDataTable) {
			executeCommandsFromDataTable(orientDBPool, inData, exec, dataTableSpec, container);
		} else if (useColumnWithCommand) {
			executeCommandsFromColumn(orientDBPool, inData, exec, container);
		}

		container.close();
		BufferedDataTable out = container.getTable();
		return new PortObject[] { FlowVariablePortObject.INSTANCE, out, orientDBConnectionPortObject };
	}

	private void executeCommandsFromColumn(ODatabasePool orientDBPool, final PortObject[] inData,
			final ExecutionContext exec, BufferedDataContainer container) {
		String columnWithCommand = m_column_with_command.getStringValue();
		BufferedDataTable dataTable = (BufferedDataTable) inData[DATA_TABLE_INDEX];
		List<String> columns = Arrays.asList(dataTable.getDataTableSpec().getColumnNames());
		int columnIndex = columns.indexOf(columnWithCommand);
		AtomicLong rowCounter = new AtomicLong();
		long commandsCount = dataTable.size();
		
		int maxConnections = isParallelExecution() ? Math.min(OrientDbUtil.getMaxPoolSize(),Runtime.getRuntime().availableProcessors()) : 1;
		logger.infoWithFormat("Max connections: %s", maxConnections);
		Function<DataRow, Callable<DataRow>> convertToCallable = new Function<DataRow, Callable<DataRow>>() {
			@Override
			public Callable<DataRow> apply(DataRow currectDataRow) {
				return new Callable<DataRow>() {
					@Override
					public DataRow call() throws Exception {
						String command = ((StringCell) currectDataRow.getCell(columnIndex)).getStringValue();
						List<DataCell> currentCells = currectDataRow.stream()
								.collect(Collectors.toCollection(ArrayList::new));
						try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
							try (OResultSet resultSet = databaseSession.command(command)) {
								if (resultSet.hasNext()) {
									OResult oResult = resultSet.next();
									currentCells.add(Constants.JSON_CELL_FACTORY.createCell(oResult.toJSON()));
								} else {
									currentCells.add(Constants.JSON_CELL_FACTORY.createCell(
											"{\"result\":\"command '" + command + "' didn't return result \"}"));
								}
							} catch (Exception e) {
								currentCells.add(Constants.JSON_CELL_FACTORY.createCell(prepareErrorMessage(e)));
							}

						}
						return new DefaultRow(currectDataRow.getKey(), currentCells);
					}
				};
			}
		};
		
		try (CloseableRowIterator rowItertor = dataTable.iterator()) {
			List<DataRow> sourceDataRows = new ArrayList<DataRow>(maxConnections);
			while (rowItertor.hasNext()) {			
				final DataRow dataRow = rowItertor.next();
				sourceDataRows.add(dataRow);
				if (sourceDataRows.size() == maxConnections) {
					List<Callable<DataRow>> tasks = sourceDataRows.stream().map(convertToCallable)
							.collect(Collectors.toList());
					sourceDataRows.clear();
					processTasks(tasks,rowCounter,commandsCount,exec,container);
				}

			}
			if (!sourceDataRows.isEmpty()) {
				logger.infoWithFormat("last partision: %s", sourceDataRows.size());
				List<Callable<DataRow>> tasks = sourceDataRows.stream().map(convertToCallable)
						.collect(Collectors.toList());
				sourceDataRows.clear();
				processTasks(tasks,rowCounter,commandsCount,exec,container);
				
			}
		} catch (Exception e) {
			logger.error("Cannot process commands from datatable!", e);
		}
		 
	}

	private void executeCommandsFromDataTable(ODatabasePool orientDBPool, final PortObject[] inData,
			final ExecutionContext exec, DataTableSpec dataTableSpec, BufferedDataContainer container)
			throws CanceledExecutionException {
		String className = m_class.getStringValue();
		logger.infoWithFormat("Execute set of upsert commands for class: %s", className);
		int maxConnections = isParallelExecution() ? Math.min(OrientDbUtil.getMaxPoolSize(),Runtime.getRuntime().availableProcessors()) : 1;
		logger.infoWithFormat("Max connections: %s", maxConnections);
		Function<DataRow, Callable<DataRow>> convertToCallable = new Function<DataRow, Callable<DataRow>>() {
			@Override
			public Callable<DataRow> apply(DataRow currectDataRow) {
				return new Callable<DataRow>() {
					@Override
					public DataRow call() throws Exception {
						List<DataCell> currentCells = new LinkedList<>();
						currentCells.addAll(currectDataRow.stream().collect(Collectors.toList()));
						try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
							String resultJson = saveDataRow(databaseSession, m_class.getStringValue(), dataTableSpec,
									currectDataRow);
							currentCells.add(Constants.JSON_CELL_FACTORY.createCell(resultJson));
						}
						return new DefaultRow(currectDataRow.getKey(), currentCells);
					}
				};

			}

		};

		BufferedDataTable dataTable = (BufferedDataTable) inData[DATA_TABLE_INDEX];
		AtomicLong rowCounter = new AtomicLong();
		long commandsCount = dataTable.size();

		try (CloseableRowIterator rowItertor = dataTable.iterator()) {		
			List<DataRow> sourceDataRows = new ArrayList<DataRow>(maxConnections);
			while (rowItertor.hasNext()) {
				final DataRow dataRow = rowItertor.next();
				sourceDataRows.add(dataRow);
				if (sourceDataRows.size() == maxConnections) {
					List<Callable<DataRow>> tasks = sourceDataRows.stream().map(convertToCallable)
							.collect(Collectors.toList());
					sourceDataRows.clear();
					processTasks(tasks,rowCounter,commandsCount,exec,container);
				}
			}
			
			if (!sourceDataRows.isEmpty()) {
				logger.infoWithFormat("last partision: %s", sourceDataRows.size());
				List<Callable<DataRow>> tasks = sourceDataRows.stream().map(convertToCallable)
						.collect(Collectors.toList());
				sourceDataRows.clear();
				processTasks(tasks,rowCounter,commandsCount,exec,container);
				
			}
		}
	}
	
	private void processTasks(List<Callable<DataRow>> tasks,AtomicLong rowCounter,long commandsCount,final ExecutionContext exec,BufferedDataContainer container) throws CanceledExecutionException {
		List<Future<DataRow>> futures = ForkJoinPool.commonPool().invokeAll(tasks);
		for (Future<DataRow> future : futures) {
			while (!FutureUtil.isFinish(future)) {
				// wait result of execution
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					
				}
			}
			DataRow newDataRow = null;
			try {
				newDataRow = future.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Cannot execute future!", e);
			}
			container.addRowToTable(newDataRow);
			exec.checkCanceled();
			rowCounter.incrementAndGet();
			exec.setProgress(rowCounter.doubleValue() / commandsCount,
					"Executed " + rowCounter + " command");

		}
		
	}

	private void executeSingleCommand(ODatabasePool orientDBPool, BufferedDataContainer container) {
		String commandTemplate = m_command.getStringValue();
		String command = FlowVariableResolver.parse(commandTemplate, this);
		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			try (OResultSet resultSet = databaseSession.command(command)) {
				if (resultSet.hasNext()) {
					OResult oResult = resultSet.next();
					pushFlowVariableString(COMMAND_RESULT_COLUMN, oResult.toJSON());
					DataRow newRow = new DefaultRow(COMMAND_RESULT_COLUMN,
							Constants.JSON_CELL_FACTORY.createCell(oResult.toJSON()));
					container.addRowToTable(newRow);
				} else {
					DataRow newRow = new DefaultRow(COMMAND_RESULT_COLUMN,
							Constants.JSON_CELL_FACTORY.createCell("{\"result\":\"command didn't return result \"}"));
					container.addRowToTable(newRow);
				}
			} catch (Exception e) {
				DataRow newRow = new DefaultRow(COMMAND_RESULT_COLUMN,
						Constants.JSON_CELL_FACTORY.createCell(prepareErrorMessage(e)));
				container.addRowToTable(newRow);
			}
		}
	}
	private boolean isParallelExecution() {
		return this.m_execution.equals(USE_PARALLEL_EXECUTION);
	}
    
    private String prepareErrorMessage(Exception e) {
    	String json = "{\"result\":\"command return error\",\"error\" : \""+e.getMessage()+"\"}";
    	return json;
    }
    
    private String saveDataRow(ODatabaseSession databaseSession, String className, DataTableSpec dataTableSpec,DataRow dataRow) {
    	OMetadata orientdbDatabaseMetadata = databaseSession.getMetadata();
    	 OClass orientDbClass = orientdbDatabaseMetadata.getSchema().getClass(className);
    	OElement saveElement = null;
    	if (orientDbClass.isEdgeType()) {
    		throw new UnsupportedOperationException("For create edge use query!");    		
    	} else if (orientDbClass.isVertexType()) {
    		saveElement = databaseSession.newVertex(orientDbClass);    		
    	} else {
    		saveElement = databaseSession.newInstance(className);
    	}
    	
    	List<String> excludeList = m_fields.getExcludeList();
		for (int index = 0; index < dataTableSpec.getColumnNames().length; index++) {
			String columnName = dataTableSpec.getColumnNames()[index];
			if (excludeList.contains(columnName) || columnName.equals(COMMAND_RESULT_COLUMN)) {
				continue;
			}
			DataColumnSpec dataColumnSpec = dataTableSpec.getColumnSpec(index);
			DataType dataType = dataColumnSpec.getType();
			if (!dataRow.getCell(index).isMissing()) {
				if (dataType.equals(StringCell.TYPE)) {
					StringCell cell = (StringCell) dataRow.getCell(index);
					saveElement.setProperty(columnName, cell.getStringValue(), OType.STRING);
				} else if (dataType.equals(LongCell.TYPE)) {
					LongCell cell = (LongCell) dataRow.getCell(index);
					saveElement.setProperty(columnName, cell.getLongValue(), OType.LONG);
				} else if (dataType.equals(BooleanCell.TYPE)) {
					BooleanCell cell = (BooleanCell) dataRow.getCell(index);
					saveElement.setProperty(columnName, cell.getBooleanValue(), OType.BOOLEAN);
				} else if (dataType.equals(IntCell.TYPE)) {
					IntCell cell = (IntCell) dataRow.getCell(index);
					saveElement.setProperty(columnName, cell.getIntValue(), OType.INTEGER);
				} else if (dataType.equals(DoubleCell.TYPE)) {
					DoubleCell cell = (DoubleCell) dataRow.getCell(index);
					saveElement.setProperty(columnName, cell.getDoubleValue(), OType.DOUBLE);
				} else if (dataType.equals(DateAndTimeCell.TYPE)) {
					DateAndTimeCell cell = (DateAndTimeCell) dataRow.getCell(index);
					long utcTimeInMillis = cell.getUTCTimeInMillis();
					saveElement.setProperty(columnName, new Date(utcTimeInMillis), OType.DATETIME);
				} else if (dataType.equals(DataType.getType(LocalDateTimeCell.class))) {
					LocalDateTimeCell cell = (LocalDateTimeCell) dataRow.getCell(index);
					LocalDateTime localDateTime = cell.getLocalDateTime();
					saveElement.setProperty(columnName, DateTimeUtils.toDate(localDateTime), OType.DATETIME);
				} else if (dataType.equals(DataType.getType(LocalDateCell.class))) {
					LocalDateCell cell = (LocalDateCell) dataRow.getCell(index);
					LocalDate localDate = cell.getLocalDate();
					saveElement.setProperty(columnName, DateTimeUtils.toDate(localDate), OType.DATETIME);
				} else if (dataType.equals(DataType.getType(LocalDateCell.class))) {
					LocalDateCell cell = (LocalDateCell) dataRow.getCell(index);
					LocalDate localDate = cell.getLocalDate();
					saveElement.setProperty(columnName, DateTimeUtils.toDate(localDate), OType.DATETIME);
				} else if (dataType.equals(DataType.getType(ZonedDateTimeCell.class))) {
					ZonedDateTimeCell cell = (ZonedDateTimeCell) dataRow.getCell(index);
					ZonedDateTime zonedDateTime = cell.getZonedDateTime();
					saveElement.setProperty(columnName, DateTimeUtils.toDate(zonedDateTime), OType.DATETIME);
				}  else {
					// @TODO support other types
					throw new UnsupportedOperationException("Unsupported cell type " + dataType + " (class : "+dataType.getCellClass().getName()+") !");
				}
			}
		}
		
		String resultJson = "";
		try {
			ORecord saveResult = saveElement.save();
			resultJson = saveResult.toJSON();			
		} catch (ORecordDuplicatedException de) {
			if (m_use_upsert.getBooleanValue()) {
				// we must update the object
				resultJson = processUpsert(databaseSession, de, m_fields.getIncludeList(), saveElement);
			} else {
				// we must create error message
				resultJson = "{\"result\":\"Record dublicated!\",\"index\":\"" + de.getIndexName() + "\"}";
			}
		}
		return resultJson;
    }
    
    private String processUpsert( ODatabaseSession databaseSession,ORecordDuplicatedException de, List<String> savedFields,OElement saveElement) {    	
    	 OElement oldElement = databaseSession.load(de.getRid());
    	 for (String fieldName : savedFields) {
    		 oldElement.setProperty(fieldName, saveElement.getProperty(fieldName));
    	 }
    	 return "{\"result\":"+oldElement.save().toJSON()+",\"status\":\"upsert\"}";
    }
    
        
    private DateAndTimeCell createCell(Date value,SimpleDateFormat format) {    	
    	return  (DateAndTimeCell) DateAndTimeCellFactory.create(format.format(value));    	
    }
    
    private DataCell mapToDataCell(OResult result, String fieldName, DataColumnSpec columnSpec, SimpleDateFormat dateFormat,
			SimpleDateFormat dateTimeFormat) {
    	DataCell cell = null;
    	if (result.getProperty(fieldName) == null) {
			cell = new MissingCell("No value");
		} else {
			DataType dataType = columnSpec.getType();
			if (dataType.equals(StringCell.TYPE)) {
				Object value = result.getProperty(fieldName);
				if (value instanceof ORecordId) {
					ORecordId rid = (ORecordId) value;
					cell = new StringCell(rid.toString());	
				} else {
					cell = new StringCell((String) value);					
				}				
			} else if (dataType.equals(LongCell.TYPE)) {
				cell = new LongCell(result.getProperty(fieldName));
			} else if (dataType.equals(IntCell.TYPE)) {
				cell = new IntCell(result.getProperty(fieldName));
			} else if (dataType.equals(BooleanCell.TYPE)) {
				Boolean value = result.getProperty(fieldName);
				cell = (value ? BooleanCell.TRUE : BooleanCell.FALSE);
			} else if (dataType.equals(DateAndTimeCell.TYPE)) {
				Date dateTimeValue = result.getProperty(fieldName);
				cell = (createCell(dateTimeValue, dateTimeFormat));					
			} else if (dataType.equals(ListCell.getCollectionType(StringCell.TYPE))) {
				//it is string array
				ORidBag values = result.getProperty(fieldName);
				Iterator<OIdentifiable> it = values.iterator();
				List<StringCell> cells  = new LinkedList<>();
				while (it.hasNext()) {
					OIdentifiable identifiable = it.next();
					ORID rid = identifiable.getIdentity();
					cells.add(new StringCell(rid.toString()));
				}				
				cell = CollectionCellFactory.createListCell(cells);					
			} 
			else {
				logger.warnWithFormat("unsupported dataType name : %s . class: %s .field: %s ",dataType.getName(), dataType, fieldName);
			}
			
		}
    	return cell;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // TODO Code executed on reset.
        // Models build during execute are cleared here.
        // Also data handled in load/saveInternals will be erased here.
//    	setConfiguredTableSpec(null);
    }

    
   	/**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {   
    	this.m_mode.saveSettingsTo(settings);
    	this.m_execution.saveSettingsTo(settings);
    	this.m_command.saveSettingsTo(settings);
        this.m_class.saveSettingsTo(settings);
        this.m_column_with_command.saveSettingsTo(settings);
        this.m_fields.saveSettingsTo(settings);
        this.connectionSettings.saveConnection(settings);
        this.m_use_upsert.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException { 
    	this.m_mode.loadSettingsFrom(settings);
    	this.m_execution.loadSettingsFrom(settings);
    	this.m_command.loadSettingsFrom(settings);
        this.m_class.loadSettingsFrom(settings);
        this.m_column_with_command.loadSettingsFrom(settings);
        this.m_fields.loadSettingsFrom(settings);
        this.m_use_upsert.loadSettingsFrom(settings);
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
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {   
    	m_mode.validateSettings(settings);
        m_command.validateSettings(settings);
        m_class.validateSettings(settings);
        m_column_with_command.validateSettings(settings);
        m_fields.validateSettings(settings);
        this.m_use_upsert.validateSettings(settings);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {        
    	logger.info(" call loadInternals ");

    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {       
    	logger.info(" call saveInternals ");
    }


	private DataTableSpec getConfiguredTableSpec() {
		return configuredTableSpec;
	}


	private void setConfiguredTableSpec(DataTableSpec configuredTableSpec) {
		this.configuredTableSpec = configuredTableSpec;
	}

	public OrientDBConnectionSettings getConnectionSettings() {
		return connectionSettings;
	}

	private void setConnectionSettings(OrientDBConnectionSettings connectionSettings) {
		this.connectionSettings = connectionSettings;
		
	}
    

}

