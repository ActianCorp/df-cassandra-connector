package com.actian.ilabs.dataflow.cassandra.reader;

public final class Split {
	
	private String node;
	private Object[] parameters;
	
	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public Object[] getParameters() {
		return parameters;
	}

	public void setParameters(Object[] parameters) {
		this.parameters = parameters;
	}
}