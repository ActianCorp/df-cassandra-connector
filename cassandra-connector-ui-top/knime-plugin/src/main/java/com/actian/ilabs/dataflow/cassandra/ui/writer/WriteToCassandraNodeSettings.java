package com.actian.ilabs.dataflow.cassandra.ui.writer;

import com.actian.ilabs.dataflow.cassandra.writer.WriteToCassandra;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import java.util.Arrays;
import java.util.List;

/*package*/ final class WriteToCassandraNodeSettings extends AbstractDRSettingsModel<WriteToCassandra> {

    public final SettingsModelString nodes = new SettingsModelString("nodes", null);
    public final SettingsModelString user = new SettingsModelString("user", null);
    public final SettingsModelString password = new SettingsModelString("password", null);
    public final SettingsModelString insertStatement = new SettingsModelString("insertStatement", null);
        
    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(nodes, user, password, insertStatement);
    }

    @Override
    public void configure(PortMetadata[] inputTypes, WriteToCassandra operator) throws InvalidSettingsException {
    	if (this.nodes.getStringValue() == null || this.nodes.getStringValue().trim().isEmpty()) {
    		throw new InvalidSettingsException("List of nodes must not be empty!");
    	}
    	
    	String[] splits = this.nodes.getStringValue().split(",");
    	for (String split : splits) {
    		if (split.trim().isEmpty()) {
    			throw new InvalidSettingsException("A node's name must not be empty!");
    		}
    	}
    	
    	if (this.insertStatement.getStringValue() == null || this.insertStatement.getStringValue().trim().isEmpty()) {
    		throw new InvalidSettingsException("Insert statement must not be empty!");
    	}
    	
    	operator.setNodes(splits);
    	operator.setUser(this.user.getStringValue());
        operator.setPassword(this.password.getStringValue());
        operator.setInsertStatement(this.insertStatement.getStringValue());
    }
}
