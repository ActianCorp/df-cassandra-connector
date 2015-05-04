package com.datastax.driver.core;

import com.datastax.driver.core.ColumnDefinitions.Definition;

/**
 * This class is a very convenient but yet 
 * nasty hack. It should only be used for
 * experimental purposes.
 */
public class PreparedIdHelper {
	
	public static ColumnDefinitions getResultSetMetadata(PreparedId id) {
		if (id.resultSetMetadata == null) {
			return new ColumnDefinitions(new Definition[]{});
		}
		return id.resultSetMetadata;
	}
}
