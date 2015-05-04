package com.actian.ilabs.dataflow.cassandra.query;

import com.pervasive.datarush.types.RecordTokenType;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class Query {

	@JsonProperty private final String queryString; 
	
	@JsonProperty private final RecordTokenType inputType;
	@JsonProperty private final RecordTokenType outputType;
	
	@JsonProperty private final Class<?>[] javaInputTypes;
	@JsonProperty private final Class<?>[] javaOutputTypes;
	
	@JsonIgnore private final Map<String, Class<?>> nameToJavaInputType;
	@JsonIgnore private final Map<String, Class<?>> nameToJavaOutputType;
	
	@JsonCreator
	public Query(
			@JsonProperty("queryString") String queryString,
			@JsonProperty("inputType") RecordTokenType inputType,
			@JsonProperty("outputType") RecordTokenType outputType,
			@JsonProperty("javaInputTypes") Class<?>[] javaInputTypes,
			@JsonProperty("javaOutputTypes") Class<?>[] javaOutputTypes)  
	{
		this.queryString = queryString;
		
		this.inputType = inputType;
		this.outputType = outputType;
		
		this.javaInputTypes = javaInputTypes;
		this.javaOutputTypes = javaOutputTypes;
		
		this.nameToJavaInputType = new HashMap<String, Class<?>>();
		for (int i = 0; i < javaInputTypes.length; i ++) {
			nameToJavaInputType.put(inputType.getName(i), javaInputTypes[i]);
		}
		
		this.nameToJavaOutputType = new HashMap<String, Class<?>>();
		for (int i = 0; i < javaOutputTypes.length; i++) {
			nameToJavaOutputType.put(outputType.getName(i), javaOutputTypes[i]);
		}
	}

	public String getQueryString() {
		return queryString;
	}

	public RecordTokenType getInputType() {
		return inputType;
	}

	public RecordTokenType getOutputType() {
		return outputType;
	}

	public Class<?>[] getJavaInputTypes() {
		return javaInputTypes;
	}

	public Class<?>[] getJavaOutputTypes() {
		return javaOutputTypes;
	}

	@JsonIgnore
	public String[] getOutputNames() {
		return outputType.getNames();
	}
	
	@JsonIgnore
	public String[] getInputNames() {
		return inputType.getNames();
	}
	
	@JsonIgnore
	public boolean isParametrized() {
		return getInputNames().length > 0;
	}
	
	public Class<?> javaInputType(String fieldName) {
		return nameToJavaInputType.get(fieldName);
	}
	
	public Class<?> javaOuputType(String fieldName) {
		return nameToJavaOutputType.get(fieldName);
	}
}
