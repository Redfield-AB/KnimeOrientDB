package se.redfield.node.port.orientdb.command;

import org.knime.core.node.NodeView;

/**
 * <code>NodeView</code> for the "OrientDBSelect" Node.
 * 
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBCommandNodeView extends NodeView<OrientDBCommandNodeModel> {

    /**
     * Creates a new view.
     * 
     * @param nodeModel The model (class: {@link OrientDBCommandNodeModel})
     */
    protected OrientDBCommandNodeView(final OrientDBCommandNodeModel nodeModel) {
        super(nodeModel);

        // TODO instantiate the components of the view here.

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void modelChanged() {

        // TODO retrieve the new model from your nodemodel and 
        // update the view.
        OrientDBCommandNodeModel nodeModel = 
            (OrientDBCommandNodeModel)getNodeModel();
        assert nodeModel != null;
        
        // be aware of a possibly not executed nodeModel! The data you retrieve
        // from your nodemodel could be null, emtpy, or invalid in any kind.
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onClose() {
    
        // TODO things to do when closing the view
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onOpen() {

        // TODO things to do when opening the view
    }

}

