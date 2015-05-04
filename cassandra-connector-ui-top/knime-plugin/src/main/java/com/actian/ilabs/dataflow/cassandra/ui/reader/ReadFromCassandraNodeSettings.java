package com.actian.ilabs.dataflow.cassandra.ui.reader;

import com.actian.ilabs.dataflow.cassandra.reader.ReadFromCassandra;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import java.util.Arrays;
import java.util.List;

/*package*/ final class ReadFromCassandraNodeSettings extends AbstractDRSettingsModel<ReadFromCassandra> {

    public final SettingsModelString nodes = new SettingsModelString("nodes", null);
    public final SettingsModelString user = new SettingsModelString("user", null);
    public final SettingsModelString password = new SettingsModelString("password", null);
    public final SettingsModelString query = new SettingsModelString("query", null);
        
    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(nodes, user, password, query);
    }

    @Override
    public void configure(PortMetadata[] inputTypes, ReadFromCassandra operator) throws InvalidSettingsException {
    	if (this.nodes.getStringValue() == null || this.nodes.getStringValue().trim().isEmpty()) {
    		throw new InvalidSettingsException("List of nodes must not be empty!");
    	}
    	
    	String[] splits = this.nodes.getStringValue().split(",");
    	for (String split : splits) {
    		if (split.trim().isEmpty()) {
    			throw new InvalidSettingsException("A node's name must not be empty!");
    		}
    	}
    	
    	if (this.query.getStringValue() == null || this.query.getStringValue().trim().isEmpty()) {
    		throw new InvalidSettingsException("Query must not be empty!");
    	}
    	
    	operator.setNodes(splits);
    	operator.setUser(this.user.getStringValue());
        operator.setPassword(this.password.getStringValue());
        operator.setQuery(this.query.getStringValue());
    }
}
