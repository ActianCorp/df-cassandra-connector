package com.actian.ilabs.dataflow.cassandra.reader;

import com.actian.ilabs.dataflow.cassandra.getter.Getter;
import com.actian.ilabs.dataflow.cassandra.query.Query;
import com.actian.ilabs.dataflow.cassandra.utils.Ring;
import com.pervasive.datarush.cal.NodeAllocationPlan.AssignmentPlan;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.ObjectOutputField;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.record.RecordPort;

import java.util.List;

import static com.actian.ilabs.dataflow.cassandra.getter.Getters.getters;
import static com.pervasive.datarush.types.TokenTypeConstant.OBJECT;
import static com.pervasive.datarush.types.TokenTypeConstant.record;

public class AssignSplits extends ExecutableOperator {

	private final RecordPort input = newRecordInput("input", true);
	private final RecordPort output = newRecordOutput("output");

	private Query query;
	private String splitsFieldName;
	private int parallelism;
	
	public RecordPort getInput() {
		return input;
	}
	
	public RecordPort getOutput() {
		return output;
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query queryMetadata) {
		this.query = queryMetadata;
	}

	public String getSplitsFieldName() {
		return splitsFieldName;
	}

	public void setSplitsFieldName(String splitsFieldName) {
		this.splitsFieldName = splitsFieldName;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	@Override
	protected void computeMetadata(StreamingMetadataContext context) {
		context.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
		output.setType(context, record(OBJECT(Split.class, splitsFieldName)));
		if (query.isParametrized()) {
			if (!context.isSourceConnected(input)) {
				throw new IllegalStateException("Parametrized query, input port must be connected!");
			}
			input.getType(context).verifyNames(query.getInputNames());
		} 
	}

	@Override
	protected void execute(ExecutionContext context) {
		Ring<String> nodes = nodesAsRing(context, parallelism);
		RecordOutput recordOutput = output.getOutput(context);
		
		@SuppressWarnings("unchecked")
		ObjectOutputField<Split> splitsField = (ObjectOutputField<Split>)recordOutput.getField(splitsFieldName);
		
		if (query.isParametrized()) {
			RecordInput recordInput = input.getInput(context);
			Getter[] getters = getters(recordInput, query.getInputNames(), query.getJavaInputTypes());
			Object[] values = new Object[getters.length];
			while(recordInput.stepNext()) {
				for (int i = 0; i < getters.length; i++) {
					values[i] = getters[i].get();
				}
				splitsField.set(split(nodes.next(), values.clone()));
				recordOutput.push();
			}
		} else {
			if (context.isSourceConnected(input)) {
				input.getInput(context).detach();
			}
			splitsField.set(split(nodes.next()));
			recordOutput.push();
		}
		recordOutput.pushEndOfData();
	}
	
	private static Split split(String node, Object... parameters) {
		Split split = new Split();
		split.setNode(node);
		split.setParameters(parameters);
		return split;
	}
	
    private static Ring<String> nodesAsRing(ExecutionContext context, int parallelismLevel) {
        List<AssignmentPlan> assignments = context.getNodeAllocation().subPlan(parallelismLevel).getAssignments();
        String[] ids = new String[assignments.size()];
        for (int i = 0; i < assignments.size(); i++) {
            ids[i] = assignments.get(i).getNodeID().toString();
        }
        return new Ring<String>(ids);
    }
}
