package se.redfield.node.port.orientdb.connection;

import java.io.IOException;

import javax.swing.JComponent;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.workflow.CredentialsProvider;

public class OrientDBConnectionPortObject implements PortObject {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBConnectionPortObject.class);
	protected final OrientDBConnectionPortObjectSpec m_spec;

	/**
	 * Database port type.
	 */
	public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(OrientDBConnectionPortObject.class);

	/**
	 * Database type for optional ports.
	 */
	public static final PortType TYPE_OPTIONAL = PortTypeRegistry.getInstance()
			.getPortType(OrientDBConnectionPortObject.class, true);

	protected OrientDBConnectionPortObject(final OrientDBConnectionPortObjectSpec conn) {
		if (conn == null) {
			throw new IllegalArgumentException("OrientDB connection model must not be null.");
		}
		m_spec = conn;
	}

	@Override
	public String getSummary() {
		return "OrientDB:OrientDBConnectionPortObject";
	}

	@Override
	public OrientDBConnectionPortObjectSpec getSpec() {
		// TODO Auto-generated method stub
		return m_spec;
	}

	@Override
	public JComponent[] getViews() {
		return m_spec.getViews();
	}

	public OrientDBConnectionSettings getConnectionSettings(final CredentialsProvider credProvider)
			throws InvalidSettingsException {
		return m_spec.getConnectionSettings(credProvider);
	}
	public static PortObjectSerializer<OrientDBConnectionPortObject> getPortObjectSerializer() {
		return new Serializer();
	}

	public static final class Serializer extends PortObjectSerializer<OrientDBConnectionPortObject> {
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void savePortObject(final OrientDBConnectionPortObject portObject, final PortObjectZipOutputStream out,
				final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
			// nothing to save
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrientDBConnectionPortObject loadPortObject(final PortObjectZipInputStream in, final PortObjectSpec spec,
				final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
			return new OrientDBConnectionPortObject((OrientDBConnectionPortObjectSpec) spec);
		}
	}

}
