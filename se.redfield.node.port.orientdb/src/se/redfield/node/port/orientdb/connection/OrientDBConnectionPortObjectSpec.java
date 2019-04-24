package se.redfield.node.port.orientdb.connection;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.workflow.CredentialsProvider;

public class OrientDBConnectionPortObjectSpec implements PortObjectSpec {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBConnectionPortObjectSpec.class);

	private final ModelContentRO m_conn;

	public OrientDBConnectionPortObjectSpec(ModelContentRO modelContent) {
		this.m_conn = modelContent;
	}
	
	public OrientDBConnectionPortObjectSpec(OrientDBConnectionSettings connSettings) {
		ModelContent temp = new ModelContent(this.getClass().getName());
        connSettings.saveConnection(temp);
        m_conn = temp;
	}
	
	public Set<String> keySet() {
		return m_conn.keySet();
	}
	
	ModelContentRO getConn() {
		return m_conn;
	}
	

	public static void saveModelContent(PortObjectSpecZipOutputStream out,
			OrientDBConnectionPortObjectSpec portObjectSpec) throws IOException {
		out.putNextEntry(new ZipEntry(OrientDBConnectionSettings.KEY_ORIENTDB_CONNECTION));
		portObjectSpec.m_conn.saveToXML(new NonClosableOutputStream.Zip(out));

	}

	public static ModelContentRO loadModelContent(PortObjectSpecZipInputStream in) throws IOException {
		ZipEntry ze = in.getNextEntry();
		if (!ze.getName().equals(OrientDBConnectionSettings.KEY_ORIENTDB_CONNECTION)) {
			throw new IOException("Key \"" + ze.getName() + "\" does not match expected zip entry name \""
					+ OrientDBConnectionSettings.KEY_ORIENTDB_CONNECTION + "\".");
		}
		return ModelContent.loadFromXML(new NonClosableInputStream.Zip(in));
	}

	@Override
	public JComponent[] getViews() {
		return new JComponent[] { new OrientDBConnectionView(m_conn) };
	}

	public OrientDBConnectionSettings getConnectionSettings(CredentialsProvider credProvider) throws InvalidSettingsException {
		 return new OrientDBConnectionSettings(m_conn, credProvider);
	}
	
	public static PortObjectSpecSerializer<OrientDBConnectionPortObjectSpec> getPortObjectSpecSerializer() {
		return new Serializer();
	}
	
	public static class Serializer extends PortObjectSpecSerializer<OrientDBConnectionPortObjectSpec> {
		@Override
		public OrientDBConnectionPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in)
				throws IOException {
			ModelContentRO modelContent = loadModelContent(in);
			return new OrientDBConnectionPortObjectSpec(modelContent);
		}

		@Override
		public void savePortObjectSpec(final OrientDBConnectionPortObjectSpec portObjectSpec,
				final PortObjectSpecZipOutputStream out) throws IOException {
			saveModelContent(out, portObjectSpec);
		}
	}

}
