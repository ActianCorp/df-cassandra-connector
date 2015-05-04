package com.actian.ilabs.dataflow.cassandra.query;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.PreparedIdHelper;
import com.datastax.driver.core.PreparedStatement;
import com.actian.ilabs.dataflow.cassandra.connection.ClusterSession;
import com.actian.ilabs.dataflow.cassandra.utils.Pair;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.ScalarTokenType;

import java.util.ArrayList;
import java.util.List;

import static com.actian.ilabs.dataflow.cassandra.setter.SetterFactory.targetTokenType;
import static com.pervasive.datarush.types.TokenTypeConstant.field;
import static com.pervasive.datarush.types.TokenTypeConstant.record;
import static com.pervasive.datarush.util.CollectionUtil.toArray;

public class Queries {

	public static Query query(ClusterSession session, String queryString) {
		PreparedStatement prepared = session.prepare(queryString);
		return query(prepared);
	}
	
	public static Query query(PreparedStatement prepared) {
		ColumnDefinitions inputColumns = prepared.getVariables();
		ColumnDefinitions outputColumns = PreparedIdHelper.getResultSetMetadata(prepared.getPreparedId());

		Pair<RecordTokenType, Class<?>[]> inputTypes = types(inputColumns);
		Pair<RecordTokenType, Class<?>[]> outputTypes = types(outputColumns);

		return new Query(prepared.getQueryString(), inputTypes.x, outputTypes.x, inputTypes.y, outputTypes.y);
	}

	private static Pair<RecordTokenType, Class<?>[]> types(ColumnDefinitions columns) {
		List<Field> fields = new ArrayList<Field>();
		List<Class<?>> originalTypes = new ArrayList<Class<?>>();
		for (Definition definition : columns) {
			String name = definition.getName();
			Class<?> javaType = definition.getType().asJavaClass();
			ScalarTokenType type = targetTokenType(javaType);
			fields.add(field(type, name));
			originalTypes.add(javaType);
		}
		return Pair.of(record(toArray(fields, Field.class)), originalTypes.toArray(new Class<?>[originalTypes.size()]));
	}
}
