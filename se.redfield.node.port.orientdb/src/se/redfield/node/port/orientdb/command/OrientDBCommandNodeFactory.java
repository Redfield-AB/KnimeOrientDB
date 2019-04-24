package se.redfield.node.port.orientdb.command;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "OrientDBNodeTest" Node.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBCommandNodeFactory extends NodeFactory<OrientDBCommandNodeModel> {
	
	@Override
    public OrientDBCommandNodeModel createNodeModel() {
        return new OrientDBCommandNodeModel();
    }

    @Override
    public int getNrNodeViews() {
        return 1;
    }

  
    @Override
    public NodeView<OrientDBCommandNodeModel> createNodeView(final int viewIndex, final OrientDBCommandNodeModel nodeModel) {
        return new OrientDBCommandNodeView(nodeModel);
    }

  
    @Override
    public boolean hasDialog() {
        return true;
    }

   
    @Override
    public NodeDialogPane createNodeDialogPane() {
        try {
			return new OrientDBCommandNodeDialog();
		} catch (InvalidSettingsException e) {
			throw new RuntimeException(e);
		}
    }

}

