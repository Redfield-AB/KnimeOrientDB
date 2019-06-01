package se.redfield.node.port.orientdb.query;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
import org.knime.core.data.RowKey;
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

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import se.redfield.node.port.orientdb.Constants;
import se.redfield.node.port.orientdb.Mapping;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObject;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionPortObjectSpec;
import se.redfield.node.port.orientdb.connection.OrientDBConnectionSettings;
import se.redfield.node.port.orientdb.util.CredentionalUtil;
import se.redfield.node.port.orientdb.util.FutureUtil;
import se.redfield.node.port.orientdb.util.OrientDbUtil;


/**
 * This is the model implementation of OrientDBNodeTest.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBQueryNodeModel extends NodeModel implements FlowVariableProvider {
    
    // the logger instance
	public static final String CFGKEY_QUERY = "Query";
	public static final String CFGKEY_GENERATE_ROWID_BY_RID = "Generate RowId by @rid?";
	public static final String CFGKEY_USE_PARALLEL = "USE_PARALLEL";
	public static final int ANALYZE_ROWS_COUNT = 100;
	
	static final int ORIENTDB_CONNECTION_INDEX = 1;
	static final int DATA_TABLE_INDEX = 0;
	
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBQueryNodeModel.class);
    
    private DataTableSpec configuredTableSpec = null;
    private final SettingsModelString m_query = new SettingsModelString(CFGKEY_QUERY, null);
    private final SettingsModelBoolean m_generate_rowid_by_rid = new SettingsModelBoolean(CFGKEY_GENERATE_ROWID_BY_RID, Boolean.FALSE);
    private final SettingsModelString m_schema_source = new SettingsModelString(OrientDBQueryNodeDialog.CFGKEY_SCHEMA_SOURCE, OrientDBQueryNodeDialog.DEFAUT_SCHEMA_SOURCE);
    private final SettingsModelBoolean m_use_parallel= new SettingsModelBoolean(CFGKEY_USE_PARALLEL, Boolean.FALSE);
    private final SettingsModelString m_column_with_query = new SettingsModelString(OrientDBQueryNodeDialog.CFGKEY_QUERY_FIELD, null);
    
    
    /**
     * Constructor for the node model.
     */
    protected OrientDBQueryNodeModel() {    
    	super( new PortType[]{BufferedDataTable.TYPE_OPTIONAL,OrientDBConnectionPortObject.TYPE}, 
    			new PortType[] { BufferedDataTable.TYPE,OrientDBConnectionPortObject.TYPE});
    }
    

	/**
     * {@inheritDoc}
     */
    @Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		OrientDBConnectionPortObject orientDBConnectionPortObject = (OrientDBConnectionPortObject) inData[ORIENTDB_CONNECTION_INDEX];
		logger.infoWithFormat("orientDBConnectionPortObject: %s", orientDBConnectionPortObject);
		OrientDBConnectionSettings connectionSettings = orientDBConnectionPortObject
				.getConnectionSettings(getCredentialsProvider());

		logger.info("Try to create connection...");

		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(connectionSettings.getUserName(),
				connectionSettings.getPassword(), connectionSettings.getCredName(), getCredentialsProvider());
		BufferedDataContainer container = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
		AtomicLong rowCounter = new AtomicLong();
		boolean useQueryFromTableColumn = (inData[DATA_TABLE_INDEX] != null);

		try (ODatabasePool orientDBPool = new ODatabasePool(connectionSettings.getDbUrl(), connectionSettings.getDbName(),
				userLogin.getLogin(), userLogin.getDecryptedPassword())) {

			DataTableSpec dataTableSpec = getConfiguredTableSpec();
			container = exec.createDataContainer(dataTableSpec);
			// show to log
			logger.infoWithFormat("fields with type : %s ", Arrays.asList(dataTableSpec.getColumnNames()));
			if (useQueryFromTableColumn) {
				int maxConnections = isParallelExecution()
						? Math.min(OrientDbUtil.getMaxPoolSize(), Runtime.getRuntime().availableProcessors())
						: 1;
				logger.infoWithFormat("Max connections: %s", maxConnections);
				String columnWithQuery = m_column_with_query.getStringValue();
				final int columnWithQueryIndex = dataTableSpec.findColumnIndex(columnWithQuery);
				logger.infoWithFormat("Column with query : %s , %s ", columnWithQuery, columnWithQueryIndex);
				Function<DataRow, Callable<DataRow>> convertToCallable = new Function<DataRow, Callable<DataRow>>() {
					@Override
					public Callable<DataRow> apply(DataRow currectDataRow) {
						return new Callable<DataRow>() {
							@Override
							public DataRow call() throws Exception {
								String query = currectDataRow.getCell(columnWithQueryIndex).toString();
								logger.info("query : " + query);
								StringBuilder stringBuilder = new StringBuilder(10_000);
								stringBuilder.append("{\"result\":[");
								try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
									try (OResultSet resultSet = databaseSession.query(query)) {
										while (resultSet.hasNext()) {
											stringBuilder.append(resultSet.next().toJSON());
											if (resultSet.hasNext()) {
												stringBuilder.append(",");
											}
										}
									}
								}
								stringBuilder.append("]}");
								List<DataCell> cells = new LinkedList<DataCell>();
								currectDataRow.forEach(new Consumer<DataCell>() {
									@Override
									public void accept(DataCell t) {
										cells.add(t);
									}
								});
								cells.add(Constants.JSON_CELL_FACTORY.createCell(stringBuilder.toString()));
								DataRow row = new DefaultRow(currectDataRow.getKey(), cells);
								return row;
							}
						};
					}
				};

				BufferedDataTable dataTable = (BufferedDataTable) inData[DATA_TABLE_INDEX];
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
							processTasks(tasks, rowCounter, commandsCount, exec, container);
						}
					}

					if (!sourceDataRows.isEmpty()) {
						logger.infoWithFormat("last partision: %s", sourceDataRows.size());
						List<Callable<DataRow>> tasks = sourceDataRows.stream().map(convertToCallable)
								.collect(Collectors.toList());
						sourceDataRows.clear();
						processTasks(tasks, rowCounter, commandsCount, exec, container);

					}
					container.close();
				}

			} else {
				try (ODatabaseSession databaseSession = orientDBPool.acquire()) {

					logger.info("Try to execute query ...");
					String query = FlowVariableResolver.parse(m_query.getStringValue(), this);
					try (OResultSet resultSet = databaseSession.query(query)) {
						container = exec.createDataContainer(dataTableSpec);
						while (resultSet.hasNext()) {
							OResult result = resultSet.next();
							processOResult(container, dateFormat, dateTimeFormat, dataTableSpec, result, rowCounter);
							// check if the execution monitor was canceled
							exec.checkCanceled();
							rowCounter.incrementAndGet();
							exec.setProgress("Loaded " + rowCounter + " row");
						}
					}

					// once we are done, we close the container and return its table
					if (container == null) {
						// we have empty result
						container = exec.createDataContainer(new DataTableSpec());
					}
					container.close();
				} 
			}
		}

		BufferedDataTable out = container.getTable();
		return new PortObject[] { out, orientDBConnectionPortObject };
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
				logger.error("Future haven't performed!", e);
			}
			container.addRowToTable(newDataRow);
			exec.checkCanceled();
			rowCounter.incrementAndGet();
			exec.setProgress(rowCounter.doubleValue() / commandsCount,
					"Executed " + rowCounter + " command");

		}
		
	}
    
    

	private boolean isParallelExecution() {
		return m_use_parallel.getBooleanValue();
	}


	private void processOResult(BufferedDataContainer container, SimpleDateFormat dateFormat,
			SimpleDateFormat dateTimeFormat, DataTableSpec dataTableSpec, OResult result,AtomicLong rowCounter) {
		List<DataCell> cells = new LinkedList<>();
		if (m_schema_source.getStringValue().equals(OrientDBQueryNodeDialog.TO_JSON_SCHEMA_SOURCE)) {
			cells.add(Constants.JSON_CELL_FACTORY.createCell(result.toJSON()));
		} else {
			for (String columnName : dataTableSpec.getColumnNames()) {
				DataColumnSpec columnSpec = dataTableSpec.getColumnSpec(columnName);
				cells.add(mapToDataCell(result, columnName, columnSpec, dateFormat, dateTimeFormat));
			}
		}
		
		DataRow row = new DefaultRow(new RowKey("Row" + getRowIdentity(result,rowCounter)), cells);
		container.addRowToTable(row);
	}
	
	private String getRowIdentity(OResult result,AtomicLong rowCounter) {
		boolean generateRowidByRid =  m_generate_rowid_by_rid.getBooleanValue();
		Optional<ORID>  identityOpt =  result.getIdentity();
		String rowId  = null;
		if (generateRowidByRid) {
			rowId = identityOpt.isPresent() ? identityOpt.toString() : String.valueOf(rowCounter.get());				
		} else {
			rowId = String.valueOf(rowCounter.get());			
		}
		return 	rowId;
	}
        
    @SuppressWarnings("deprecation")
	private DateAndTimeCell createCell(Date value,SimpleDateFormat format) {    	
    	return  (DateAndTimeCell) DateAndTimeCellFactory.create(format.format(value));    	
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes", "static-access" })
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
			} else if (dataType.equals(DoubleCell.TYPE)) {
				cell = new DoubleCell(result.getProperty(fieldName));
			} else if (dataType.equals(IntCell.TYPE)) {
				cell = new IntCell(result.getProperty(fieldName));
			} else if (dataType.equals(BooleanCell.TYPE)) {
				Boolean value = result.getProperty(fieldName);
				cell = (value ? BooleanCell.TRUE : BooleanCell.FALSE);
			} else if (dataType.equals(DateAndTimeCell.TYPE)) {
				Date dateTimeValue = result.getProperty(fieldName);
				cell = (createCell(dateTimeValue, dateTimeFormat));	
			} else if (dataType.equals(Constants.JSON_CELL_FACTORY.getDataType())) {
				// show as is
				Object value = result.getProperty(fieldName);
				try {
					String json = "{}";
					if (value instanceof ORidBag) {
						List values = new LinkedList<>();
						ORidBag ridBag = (ORidBag) value;
						Iterator<OIdentifiable> it = ridBag.iterator();
						while (it.hasNext()) {
							OIdentifiable identifiable = it.next();
							values.add(identifiable.getIdentity().toString());
						}
						json = Constants.OBJECT_MAPPER.writeValueAsString(values);
					} else if (value instanceof List || value instanceof Set) {
						Collection col = (Collection) value;
						if (!col.isEmpty()) {
							Object firstValue = col.iterator().next();
							logger.info(col.iterator().next().getClass().getName());
							if (firstValue instanceof OResultInternal) {
								//Orientdb schema class
								@SuppressWarnings("unused")
								Collection<OResultInternal> typeColl = col;	
								OResultInternal i = typeColl.iterator().next();
								StringBuilder buffer = new StringBuilder(10_000);
								buffer.append("[");
								buffer.append(typeColl.stream().map((OResultInternal ori)->{return ori.toJSON();}).collect(Collectors.joining(",")));
								buffer.append("]");
								json = buffer.toString();
							} else {
								json = Constants.OBJECT_MAPPER.writeValueAsString(value);
							}							
						}
						
					} else {
						json = Constants.OBJECT_MAPPER.writeValueAsString(value);
					}
					cell = Constants.JSON_CELL_FACTORY.create(json, true);
				} catch (Exception e) {
					throw new RuntimeException("Cannot process JSON", e);
				}

			} else if (dataType.equals(ListCell.getCollectionType(StringCell.TYPE))) {
				//@depricated
				Object field = result.getProperty(fieldName);
				List<StringCell> cells  = new LinkedList<>();
				List values = null;
				if (field instanceof ORidBag) {
					values = new LinkedList<>();
					ORidBag ridBag = (ORidBag) field;
					Iterator<OIdentifiable> it =  ridBag.iterator();
					while (it.hasNext()) {
						OIdentifiable identifiable = it.next();
						values.add(identifiable.getIdentity().toString());
					}
				} else if (field instanceof List) {
					values = (List) field;									
				}	else if (field instanceof Set) {
					values = new LinkedList((Set) field);									
				}			
				if (values!=null) {
				for (Object value : values) {
					cells.add(new StringCell(value.toString()));
				}				
				cell = CollectionCellFactory.createListCell(cells);
				} else {
					cell = new MissingCell("No value");
				}
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
    	setConfiguredTableSpec(null);
    }

    
    @Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs == null || inSpecs.length < 1 || inSpecs[ORIENTDB_CONNECTION_INDEX] == null) {
			throw new InvalidSettingsException("No required input available!");
		}

		OrientDBConnectionPortObjectSpec orientDbSpec = (OrientDBConnectionPortObjectSpec) inSpecs[ORIENTDB_CONNECTION_INDEX];
		OrientDBConnectionSettings connectionSettings = orientDbSpec.getConnectionSettings(getCredentialsProvider());
		if (connectionSettings.getDbUrl() == null) {
			throw new InvalidSettingsException("OrientDBConnection node not configured!");
		}
		DataTableSpec tableTableSpec = (DataTableSpec) inSpecs[DATA_TABLE_INDEX];
		boolean useCommandFromTableColumn = (tableTableSpec!=null);
		if (useCommandFromTableColumn) {
			m_schema_source.setStringValue(OrientDBQueryNodeDialog.TO_JSON_SCHEMA_SOURCE);
		}
				
		logger.infoWithFormat("useCommandFromTableColumn : %s", useCommandFromTableColumn);
		
		CredentionalUtil.UserLogin userLogin = CredentionalUtil.getUserLoginInfo(connectionSettings.getUserName(), connectionSettings.getPassword(),
				connectionSettings.getCredName(), getCredentialsProvider());
		try (ODatabasePool orientDBPool = new ODatabasePool(connectionSettings.getDbUrl(), connectionSettings.getDbName(),
				userLogin.getLogin(), userLogin.getDecryptedPassword())) {
			List<DataColumnSpec> columns = defineColumns(orientDBPool);
			if (useCommandFromTableColumn) {
				List<DataColumnSpec> currentColumns = new LinkedList<DataColumnSpec>();
				for (int i = 0; i < tableTableSpec.getNumColumns(); i++) {
					currentColumns.add(tableTableSpec.getColumnSpec(i));
				}
				columns.addAll(0, currentColumns);
			}
			DataTableSpec dataTableSpec = new DataTableSpec(columns.toArray(new DataColumnSpec[columns.size()]));
			setConfiguredTableSpec(dataTableSpec);
		}

		return new PortObjectSpec[] { getConfiguredTableSpec(), orientDbSpec };
	}
    
    private List<DataColumnSpec>  defineColumns(ODatabasePool orientDBPool) {
    	List<DataColumnSpec> columns = new LinkedList<DataColumnSpec>();
		LinkedHashMap<String, OType> fieldTypeMap = new LinkedHashMap<>();
		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			if (m_schema_source.getStringValue().equals(OrientDBQueryNodeDialog.DEFAUT_SCHEMA_SOURCE)) {
				logger.info("Using query as schema info");
				if (m_query.getStringValue() != null && !m_query.getStringValue().isEmpty()) {

					logger.info("Try to execute query ...");
					String query = FlowVariableResolver.parse(m_query.getStringValue(), this);
					try (OResultSet resultSet = databaseSession.query(query)) {
						// need found row with all fields. We will analyse 100 first rows

						for (int index = 0; index < ANALYZE_ROWS_COUNT; index++) {
							if (resultSet.hasNext()) {
								OResult result = resultSet.next();
								for (String propertyName : result.getPropertyNames()) {
									logger.infoWithFormat("propertyName: %s", propertyName);
									if (result.hasProperty(propertyName)) {
										Optional propertyValueOpt = Optional.ofNullable(result.getProperty(propertyName));
										if (propertyValueOpt.isPresent()) {
											Class propertyValueClass = propertyValueOpt.get().getClass();
											logger.infoWithFormat("propertyValueClass: %s", propertyValueClass);
											OType orientDBType = OType.getTypeByClass(propertyValueClass);
											logger.infoWithFormat("orientDBType: %s ", orientDBType);
											fieldTypeMap.put(propertyName, orientDBType);
										}
									}
								}
							} else {
								break;
							}
						}
					}
				}
				logger.infoWithFormat("fields with type : %s ", fieldTypeMap);
				columns.addAll(prepareColumns(fieldTypeMap));
			} else if (m_schema_source.getStringValue().equals(OrientDBQueryNodeDialog.TO_JSON_SCHEMA_SOURCE)) {
				logger.info("Using schemaless");
				columns.add( new DataColumnSpecCreator("result", JSONCell.TYPE).createSpec());				
			} else {
				String orientdbClassName = m_schema_source.getStringValue();
				logger.infoWithFormat("Using class %s as schema info",orientdbClassName);
				OClass orientdbClass = databaseSession.getClass(orientdbClassName);
				if (orientdbClass!=null) {
					for (OProperty property : orientdbClass.properties()) {
						fieldTypeMap.put(property.getName(), property.getType());
					}					
				}	
				columns.addAll(prepareColumns(fieldTypeMap));
			}
			logger.infoWithFormat("fields with type : %s ", fieldTypeMap);
			
		} finally {
			orientDBPool.close();
		}
		return columns;
    }
    
    private List<DataColumnSpec> prepareColumns(Map<String, OType> fieldTypeMap) {
    	List<DataColumnSpec> columns = new LinkedList<DataColumnSpec>();
    	for (Map.Entry<String, OType> entry : fieldTypeMap.entrySet()) {
    		logger.info("field "+entry.getKey()+"; value "+Mapping.mapToDataType(entry.getValue()));
			DataColumnSpec columnSpec = new DataColumnSpecCreator(entry.getKey(),
					Mapping.mapToDataType(entry.getValue())).createSpec();
			columns.add(columnSpec);
		}
    	return columns;
    }
    
    	/**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {   
        m_query.saveSettingsTo(settings);
        m_generate_rowid_by_rid.saveSettingsTo(settings);
        m_schema_source.saveSettingsTo(settings);
        m_use_parallel.saveSettingsTo(settings);
		if (m_column_with_query.getStringValue() != null) {
			m_column_with_query.saveSettingsTo(settings);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_query.loadSettingsFrom(settings);
		m_generate_rowid_by_rid.loadSettingsFrom(settings);
		m_schema_source.loadSettingsFrom(settings);
		m_use_parallel.loadSettingsFrom(settings);
		try {
			m_column_with_query.loadSettingsFrom(settings);
		} catch (InvalidSettingsException e) {
			logger.info("Property "+OrientDBQueryNodeDialog.CFGKEY_QUERY_FIELD+" isn't loaded!");
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_query.validateSettings(settings);
		m_generate_rowid_by_rid.validateSettings(settings);
		m_schema_source.validateSettings(settings);
		m_use_parallel.validateSettings(settings);
		if (m_column_with_query.getStringValue() != null) {
			m_column_with_query.validateSettings(settings);
		}
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
    

}

