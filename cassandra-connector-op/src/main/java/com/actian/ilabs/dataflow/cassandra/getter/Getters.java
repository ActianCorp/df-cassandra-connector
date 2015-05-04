package com.actian.ilabs.dataflow.cassandra.getter;

import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.ScalarInputField;

import java.util.ArrayList;
import java.util.List;

import static com.actian.ilabs.dataflow.cassandra.getter.GetterFactory.getter;

public class Getters {

	public static Getter[] getters(RecordInput recordInput, String[] fieldNames, Class<?>[] targetJavaTypes) {
		ScalarInputField[] fields = fields(recordInput, fieldNames);
		List<Getter> getters = new ArrayList<Getter>();
		
		for (int i = 0; i < fields.length; i++) {
			getters.add(getter(fields[i], targetJavaTypes[i]));
		}
		return getters.toArray(new Getter[getters.size()]);
	}
	
	private static ScalarInputField[] fields(RecordInput recordInput, String[] fieldNames) {
		List<ScalarInputField> fields = new ArrayList<ScalarInputField>();
		for (String name : fieldNames) {
			fields.add(recordInput.getField(name));
		}
		return fields.toArray(new ScalarInputField[fields.size()]);
	}
}
