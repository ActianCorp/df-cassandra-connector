package com.actian.ilabs.dataflow.cassandra.reader;

import com.actian.ilabs.dataflow.cassandra.connection.ClusterSession;
import com.actian.ilabs.dataflow.cassandra.query.Queries;
import com.actian.ilabs.dataflow.cassandra.query.Query;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.CompositeOperator;
import com.pervasive.datarush.operators.CompositionContext;
import com.pervasive.datarush.operators.sink.CollectRecords;
import com.pervasive.datarush.ports.record.RecordPort;

public class ReadFromCassandra extends CompositeOperator {

	private static String SPLITS = "splits";
	 
	private final RecordPort input = newRecordInput("input", true);
	private final RecordPort output = newRecordOutput("output");
	
	private String[] nodes;
	private String query;
	private String user;
	private String password;
	
	public RecordPort getOutput() {
		return output;
	}
	
	public RecordPort getInput() {
		return input;
	}
	
	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String... nodes) {
		this.nodes = nodes;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
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
	protected void compose(CompositionContext context) {
		Query query = query();
		
		AssignSplits assigner = context.add(new AssignSplits());
		assigner.setQuery(query);
		assigner.setSplitsFieldName(SPLITS);
		assigner.setParallelism(context.getMaxParallelism());
		
		ReadSplits reader = context.add(new ReadSplits());
		reader.setQuery(query);
		reader.setSplitsFieldName(SPLITS);
		reader.setNodes(nodes);
		reader.setUser(user);
		reader.setPassword(password);
		
		context.connect(this.getInput(), assigner.getInput());
		context.connect(assigner.getOutput(), reader.getInput());
		context.connect(reader.getOutput(), this.getOutput());
	}
	
	private Query query() {
		ClusterSession session = null;
		try {
			session = new ClusterSession(nodes, user, password);
			return Queries.query(session, query);
		} finally {
			if (session != null) {
				session.close();
			}
		}
	}
	
	public static void main(String[] args) {
		LogicalGraph graph = LogicalGraphFactory.newLogicalGraph("Cassandra reader test");

		ReadFromCassandra reader = graph.add(new ReadFromCassandra());
		reader.setNodes("localhost");
		reader.setQuery("select * from mykeyspace.users");
//		reader.setQuery("select * from mykeyspace.users where user_id = ?");
		
		CollectRecords writer = graph.add(new CollectRecords());
//		graph.connect(input.getOutput(), reader.getInput());
		graph.connect(reader.getOutput(), writer.getInput());
		graph.run();

		System.out.println(writer.getOutput());
	}
}