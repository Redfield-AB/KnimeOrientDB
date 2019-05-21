package se.redfield.node.port.orientdb.execute;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "OrientDBNodeTest" Node.
 *
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBExecuteNodeFactory extends NodeFactory<OrientDBExecuteNodeModel> {
	@Override
	public OrientDBExecuteNodeModel createNodeModel() {
		return new OrientDBExecuteNodeModel();
	}

	@Override
	public int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<OrientDBExecuteNodeModel> createNodeView(final int viewIndex,
			final OrientDBExecuteNodeModel nodeModel) {
		return null;
	}

	@Override
	public boolean hasDialog() {
		return true;
	}

	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new OrientDBExecuteNodeDialog();
	}

}
