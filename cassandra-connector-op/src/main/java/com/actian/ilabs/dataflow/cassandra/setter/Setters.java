package com.actian.ilabs.dataflow.cassandra.setter;

import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarOutputField;

import java.util.ArrayList;
import java.util.List;

import static com.actian.ilabs.dataflow.cassandra.setter.SetterFactory.setter;

public class Setters {

	public static Setter[] setters(RecordOutput recordOutput, String[] fieldNames, Class<?>[] sourceJavaTypes) {
		ScalarOutputField[] fields = fields(recordOutput, fieldNames);
		List<Setter> setters = new ArrayList<Setter>();
		for (int i = 0; i < fields.length; i++) {
			setters.add(setter(fields[i], sourceJavaTypes[i]));
		}
		return setters.toArray(new Setter[setters.size()]);
	}
	
	private static ScalarOutputField[] fields(RecordOutput recordOutput, String[] fieldNames) {
		List<ScalarOutputField> fields = new ArrayList<ScalarOutputField>();
		for (String name : fieldNames) {
			fields.add(recordOutput.getField(name));
		}
		return fields.toArray(new ScalarOutputField[fields.size()]);
	}
}
