package se.redfield.node.port.orientdb.connection;

import java.awt.Dimension;

import javax.swing.GroupLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.knime.core.node.ModelContentRO;
import org.knime.core.node.NodeLogger;

import se.redfield.node.port.orientdb.OrientDBConnectionKeys;

public class OrientDBConnectionView extends JPanel {
	private static final long serialVersionUID = 1L;
	private static final NodeLogger LOGGER = NodeLogger.getLogger(OrientDBConnectionView.class);

	private javax.swing.JScrollPane jScrollPane2;
	private javax.swing.JTable jTable1;

	OrientDBConnectionView(final ModelContentRO sett) {
		setName("OrientDB Connection");
		setSize(new Dimension(200, 200));
		setPreferredSize(new Dimension(200, 200));

		initComponents(sett.getString(OrientDBConnectionKeys.CFGKEY_DB_URL, null),
				sett.getString(OrientDBConnectionKeys.CFGKEY_REMOTE_DATABASE_NAME, null),
				sett.getString(OrientDBConnectionKeys.CFGKEY_USER_NAME, ""),
				sett.getString(OrientDBConnectionKeys.CFGKEY_CREDENTIAL_NAME, ""));
	}

	private void initComponents(String connectionUrl, String databaseName, String userName, String credentialName) {

		jScrollPane2 = new JScrollPane();
		jTable1 = new JTable();
		jTable1.setModel(new javax.swing.table.DefaultTableModel(
				new Object[][] { { "Connection URL", connectionUrl }, { "Database Name", databaseName },
						{ "UserName", userName }, { "Credential Name", credentialName } },
				new String[] { "Parameter name", "Value" }));
		jScrollPane2.setViewportView(jTable1);

		GroupLayout layout = new GroupLayout(this);
		this.setLayout(layout);
		layout.setHorizontalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(layout.createSequentialGroup().addContainerGap()
						.addComponent(jScrollPane2, GroupLayout.DEFAULT_SIZE, 388, Short.MAX_VALUE).addContainerGap()));
		layout.setVerticalGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
				.addGroup(layout.createSequentialGroup().addContainerGap()
						.addComponent(jScrollPane2, GroupLayout.PREFERRED_SIZE, 391, GroupLayout.PREFERRED_SIZE)
						.addContainerGap(14, Short.MAX_VALUE)));
	}

}
