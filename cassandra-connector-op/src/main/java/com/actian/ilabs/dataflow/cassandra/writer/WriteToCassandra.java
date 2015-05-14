package com.actian.ilabs.dataflow.cassandra.writer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.actian.ilabs.dataflow.cassandra.connection.ClusterSession;
import com.actian.ilabs.dataflow.cassandra.getter.Getter;
import com.actian.ilabs.dataflow.cassandra.query.Query;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.record.RecordPort;

import static com.actian.ilabs.dataflow.cassandra.getter.Getters.getters;
import static com.actian.ilabs.dataflow.cassandra.query.Queries.query;

public class WriteToCassandra extends ExecutableOperator {

	private final int DEFAULT_BATCH_SIZE = 100; 
	
	private final RecordPort input = newRecordInput("input", true);
	
	private String[] nodes;
	private String insertStatement;
	private String user;
	private String password;
	
	public RecordPort getInput() {
		return input;
	}
	
	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}

	public String getInsertStatement() {
		return insertStatement;
	}

	public void setInsertStatement(String insertStatement) {
		this.insertStatement = insertStatement;
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
		Query query = null;
		ClusterSession session = null;
		try {
			session = new ClusterSession(nodes, user, password);
			query = query(session, insertStatement);
		} finally {
			if (session != null) {
				session.close();
			}
		}
		
		if (query.isParametrized()) {
			if (!context.isSourceConnected(input)) {
				throw new IllegalStateException("Parametrized statement, input port must be connected!");
			}
			// verifyNames causes problems with case insensitive column names
			// input.getType(context).verifyNames(query.getInputNames());
			context.parallelize(ParallelismStrategy.CONFIGURED);
		} else {
			context.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
		}
	}
	
	@Override
	protected void execute(ExecutionContext context) {
		ClusterSession session = null;
		try {
			session = new ClusterSession(nodes, user, password);
			PreparedStatement prepared = session.prepare(insertStatement);
			Query query = query(prepared);
			BatchedStatementExecutor executor = new BatchedStatementExecutor(session, DEFAULT_BATCH_SIZE);
			if (query.isParametrized()) {
				RecordInput recordInput = input.getInput(context);
				Getter[] getters = getters(recordInput, query.getInputNames(), query.getJavaInputTypes());
				Object[] values = new Object[getters.length];
				while(recordInput.stepNext()) {
					for (int i = 0; i < getters.length; i++) {
						values[i] = getters[i].get();
					}
					executor.execute(prepared.bind(values));
				}
			} else {
				if (context.isSourceConnected(input)) {
					input.getInput(context).detach();
				}
				executor.execute(prepared.bind());
			}
			executor.flush();
		} finally {
			if (session != null) {
				session.close();
			}
		}
	}
	
	private static class BatchedStatementExecutor {
		
		private final BatchStatement batch;
		private final ClusterSession session; 
		private final int batchSize;
		
		private int count = 0;

		private BatchedStatementExecutor(ClusterSession session, int batchSize) {
			this.batch = new BatchStatement();
			this.session = session;
			this.batchSize = batchSize;
		}
		
		private void execute(Statement statement) {
			batch.add(statement);
			count++;
			if (count == batchSize) {
				flush();
			}
		}
		
		private void flush() {
			session.execute(batch);
			batch.clear();
		}
	}
	
	public static void main(String[] args) {
		LogicalGraph graph = LogicalGraphFactory.newLogicalGraph();
		ReadDelimitedText reader = graph.add(new ReadDelimitedText("https://raw.githubusercontent.com/ActianCorp/df-cassandra-connector/master/examples/419234754_T_ONTIME_2015_1.csv"));
		reader.setHeader(true);
		WriteToCassandra writer = graph.add(new WriteToCassandra());
		writer.setNodes(new String[]{"localhost"});
		writer.setInsertStatement("insert into airline.ontime (\n" +
                "        FL_DATE,\n" +
                "        UNIQUE_CARRIER,\n" +
                "        FL_NUM,\n" +
                "        ORIGIN_AIRPORT_ID,\n" +
                "        ORIGIN_AIRPORT_SEQ_ID,\n" +
                "        ORIGIN_CITY_MARKET_ID,\n" +
                "        DEST_AIRPORT_ID,\n" +
                "        DEST_AIRPORT_SEQ_ID,\n" +
                "        DEST_CITY_MARKET_ID,\n" +
                "        CRS_DEP_TIME,\n" +
                "        DEP_TIME,\n" +
                "        DEP_DELAY,\n" +
                "        CRS_ARR_TIME,\n" +
                "        ARR_TIME,\n" +
                "        ARR_DELAY,\n" +
                "        CRS_ELAPSED_TIME,\n" +
                "        ACTUAL_ELAPSED_TIME)\n" +
                "        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

		graph.connect(reader.getOutput(), writer.getInput());
		graph.compile().run();
	}
}
