package com.actian.ilabs.dataflow.cassandra.ui.writer;

import com.actian.ilabs.dataflow.cassandra.writer.WriteToCassandra;
import com.pervasive.datarush.knime.core.framework.AbstractDRNodeFactory;
import com.pervasive.datarush.knime.core.framework.DRNodeModel;
import com.pervasive.datarush.knime.coreui.common.CustomDRNodeDialogPane;

public final class WriteToCassandraNodeModelFactory extends AbstractDRNodeFactory<WriteToCassandra> {

    @Override
    protected CustomDRNodeDialogPane<WriteToCassandra> createNodeDialogPane() {
    	CustomDRNodeDialogPane<WriteToCassandra> dialog = new CustomDRNodeDialogPane<WriteToCassandra>(new WriteToCassandra(), new WriteToCassandraNodeDialogPane());
    	dialog.setDefaultTabTitle("Properties");
        return dialog;
    }

    @Override
    public DRNodeModel<WriteToCassandra> createDRNodeModel() {
        return new DRNodeModel<WriteToCassandra>( new WriteToCassandra(), new WriteToCassandraNodeSettings());
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }
}
