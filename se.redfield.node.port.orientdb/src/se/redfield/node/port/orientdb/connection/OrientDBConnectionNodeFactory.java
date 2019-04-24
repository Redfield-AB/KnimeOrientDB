package se.redfield.node.port.orientdb.connection;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class OrientDBConnectionNodeFactory  extends NodeFactory<OrientDBConnectionNodeModel> {

	@Override
	public OrientDBConnectionNodeModel createNodeModel() {
		// TODO Auto-generated method stub
		return new OrientDBConnectionNodeModel();
	}

	@Override
	protected int getNrNodeViews() {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public NodeView<OrientDBConnectionNodeModel> createNodeView(int viewIndex, OrientDBConnectionNodeModel nodeModel) {
		// TODO Auto-generated method stub
		return new OrientDBConnectionNodeView(nodeModel);
	}

	@Override
	protected boolean hasDialog() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		// TODO Auto-generated method stub
		return new OrientDBConnectionNodeDialog();
	}

}
