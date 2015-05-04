package com.actian.ilabs.dataflow.cassandra.connection;

import com.datastax.driver.core.*;

public class ClusterSession {

	private final Cluster cluster;
	private final Session session;
	
	public ClusterSession(String[] nodes, String user, String password) {
		this.cluster = Cluster.builder().withCredentials(user, password).addContactPoints(nodes).build();
		this.session = cluster.connect();
	}
	
	public PreparedStatement prepare(String statement) {
		return session.prepare(statement);
	}
	
	public ResultSet execute(String statement, Object... parameters) { 
		return session.execute(statement, parameters);
	}
	
	public ResultSet execute(Statement statement) {
		return session.execute(statement);
	}
	
	public void close() {
		session.close();
		cluster.close();
	}
}