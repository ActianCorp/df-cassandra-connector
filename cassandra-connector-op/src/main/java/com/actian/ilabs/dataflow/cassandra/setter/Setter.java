package com.actian.ilabs.dataflow.cassandra.setter;

import com.datastax.driver.core.Row;

public interface Setter {

	public void set(Row row);
}
