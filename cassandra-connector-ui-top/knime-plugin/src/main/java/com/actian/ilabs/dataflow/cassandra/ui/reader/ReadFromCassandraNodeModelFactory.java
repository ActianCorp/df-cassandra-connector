package com.actian.ilabs.dataflow.cassandra.ui.reader;

import com.actian.ilabs.dataflow.cassandra.reader.ReadFromCassandra;
import com.pervasive.datarush.knime.core.framework.AbstractDRNodeFactory;
import com.pervasive.datarush.knime.core.framework.DRNodeModel;
import com.pervasive.datarush.knime.coreui.common.CustomDRNodeDialogPane;

public final class ReadFromCassandraNodeModelFactory extends AbstractDRNodeFactory<ReadFromCassandra> {

    @Override
    protected CustomDRNodeDialogPane<ReadFromCassandra> createNodeDialogPane() {
    	CustomDRNodeDialogPane<ReadFromCassandra> dialog = new CustomDRNodeDialogPane<ReadFromCassandra>(new ReadFromCassandra(), new ReadFromCassandraNodeDialogPane());
    	dialog.setDefaultTabTitle("Properties");
        return dialog;
    }

    @Override
    public DRNodeModel<ReadFromCassandra> createDRNodeModel() {
        return new DRNodeModel<ReadFromCassandra>( new ReadFromCassandra(), new ReadFromCassandraNodeSettings());
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

}
