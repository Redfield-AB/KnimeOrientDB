package se.redfield.node.port.orientdb.query;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "OrientDBNodeTest" Node.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBQueryNodeFactory extends NodeFactory<OrientDBQueryNodeModel> {

    @Override
    public OrientDBQueryNodeModel createNodeModel() {
        return new OrientDBQueryNodeModel();
    }

    @Override
    public int getNrNodeViews() {
        return 1;
    }

  
    @Override
    public NodeView<OrientDBQueryNodeModel> createNodeView(final int viewIndex, final OrientDBQueryNodeModel nodeModel) {
        return new OrientDBQueryNodeView(nodeModel);
    }

  
    @Override
    public boolean hasDialog() {
        return true;
    }

   
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new OrientDBQueryNodeDialog();
    }

}

