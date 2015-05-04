package com.actian.ilabs.dataflow.cassandra.reader;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.actian.ilabs.dataflow.cassandra.connection.ClusterSession;
import com.actian.ilabs.dataflow.cassandra.getter.GetterFactory;
import com.actian.ilabs.dataflow.cassandra.query.Query;
import com.actian.ilabs.dataflow.cassandra.setter.Setter;
import com.actian.ilabs.dataflow.cassandra.setter.Setters;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.ObjectInputField;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.record.FullDataDistribution;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.ScalarTokenType;

import java.util.ArrayList;
import java.util.List;

import static com.actian.ilabs.dataflow.cassandra.query.Queries.query;

public class ReadSplits extends ExecutableOperator {
	
	private final RecordPort input = newRecordInput("input");
	private final RecordPort output = newRecordOutput("output");
	
	private String splitsFieldName;
	
	private Query query;
	private String[] nodes;
	private String user;
	private String password;
	
	public RecordPort getInput() {
		return input;
	}
	
	public RecordPort getOutput() {
		return output;
	}

	public String getSplitsFieldName() {
		return splitsFieldName;
	}

	public void setSplitsFieldName(String splitsFieldName) {
		this.splitsFieldName = splitsFieldName;
	}
	
	public Query getQuery() {
		return query;
	}

	public void setQuery(Query queryMetadata) {
		this.query = queryMetadata;
	}

	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String... nodes) {
		this.nodes = nodes;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	@Override
	protected void computeMetadata(StreamingMetadataContext context) {
		output.setType(context, query.getOutputType());
		input.setRequiredDataDistribution(context, FullDataDistribution.INSTANCE);
		context.parallelize(ParallelismStrategy.CONFIGURED);
	}
	
	@Override
	protected void execute(ExecutionContext context) {
		RecordOutput recordOutput = output.getOutput(context);
		RecordInput recordInput = input.getInput(context);
		
		@SuppressWarnings("unchecked")
		ObjectInputField<Split> parameterField = (ObjectInputField<Split>)recordInput.getField(splitsFieldName);
		String originalNode = context.getPartitionInfo().getAssignment().getOriginalNodeID().toString();
		ClusterSession session = null;
		try {
			session = new ClusterSession(nodes, user, password);
			Setter[] setters = setters(session, recordOutput);
			while (recordInput.stepNext()) {
				Split split = parameterField.asObject();
				if (split.getNode().equals(originalNode)) {
					ResultSet result = session.execute(query.getQueryString(), split.getParameters());
					for (Row row : result) {
						for (Setter decoder : setters) {
							decoder.set(row);
						}
						recordOutput.push();
					}
				}
			}
		} finally {
			if (session != null) {
				session.close();
			}
		}
		recordOutput.pushEndOfData();
	}
	
	/**
	 * Compare the column definitions with the expected output type 
	 * and create appropriate decoders. Filter out fields, that 
	 * changed incompatibly (removed, renamed or changed the type to
	 * an incompatible type) between the the computeMetadata phase 
	 * and the execution phase. Their value will be set to null.  
	 */
	private Setter[] setters(ClusterSession session, RecordOutput recordOutput) {
		Query originalQuery = this.query;
		Query currentQuery = query(session, query.getQueryString());
		
		if (originalQuery.getOutputType().equals(currentQuery.getOutputType())) {
			return Setters.setters(recordOutput, originalQuery.getOutputNames(), originalQuery.getJavaOutputTypes());
		} else {
			List<String> outputFieldNames = new ArrayList<String>();
			List<Class<?>> outputFieldJavaTypes = new ArrayList<Class<?>>();
			
			for (String name : currentQuery.getOutputNames()) {
				Field originalField = originalQuery.getOutputType().get(name);
				if (originalField != null) {
					ScalarTokenType originalFieldType = originalField.getType();
					Class<?> currentFieldJavaType = currentQuery.javaOuputType(name);
					
					if (GetterFactory.targetJavaTypes(originalFieldType).contains(currentFieldJavaType)) {
						outputFieldNames.add(name);
						outputFieldJavaTypes.add(currentFieldJavaType);
					}
				}
			}
			return Setters.setters(recordOutput, 
					outputFieldNames.toArray(new String[outputFieldNames.size()]), 
					outputFieldJavaTypes.toArray(new Class<?>[outputFieldJavaTypes.size()])); 
		}
	}
}
