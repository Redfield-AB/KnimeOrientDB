package se.redfield.node.port.orientdb.function;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "OrientDBNodeTest" Node.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBFunctionNodeFactory extends NodeFactory<OrientDBFunctionNodeModel> {
	private static final NodeLogger logger = NodeLogger.getLogger(OrientDBFunctionNodeFactory.class);

    @Override
    public OrientDBFunctionNodeModel createNodeModel() {
        return new OrientDBFunctionNodeModel();
    }

    @Override
    public int getNrNodeViews() {
        return 1;
    }

  
    @Override
    public NodeView<OrientDBFunctionNodeModel> createNodeView(final int viewIndex, final OrientDBFunctionNodeModel nodeModel) {
        return new OrientDBFunctionNodeView(nodeModel);
    }

  
    @Override
    public boolean hasDialog() {
        return true;
    }

   
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new OrientDBFunctionNodeDialog();
    }

}

