package com.actian.ilabs.dataflow.cassandra.getter;

import com.pervasive.datarush.ports.physical.*;
import com.pervasive.datarush.types.ScalarTokenType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import static com.pervasive.datarush.types.TokenTypeConstant.*;

public class GetterFactory {
	
	private static final Map<ScalarTokenType, InternalFactory> factories;
	static {
		Map<ScalarTokenType, InternalFactory> map = new HashMap<ScalarTokenType, InternalFactory>();
		map.put(STRING, new StringGetterFactory());
		map.put(BINARY, new BinaryGetterFactory());
		
		map.put(BOOLEAN, new BooleanGetterFactory());
		
		map.put(NUMERIC, new NumericGetterFactory());
		map.put(DOUBLE, new DoubleGetterFactory());
		map.put(FLOAT, new FloatGetterFactory());
		map.put(LONG, new LongGetterFactory());
		map.put(INT, new IntegerGetterFactory());
		
		// TODO: DateGetterFactory
		// TODO: ObjectGetterFactory (InetAddress, UUID, List, Set, Map)
		factories = Collections.unmodifiableMap(map);
	}
	
	public static Getter getter(ScalarInputField source, Class<?> targetJavaType) {
		return factories.get(source.getType()).create(source, targetJavaType);
	}
	
	public static Set<Class<?>> targetJavaTypes(ScalarTokenType sourceType) {
		return factories.get(sourceType).targetJavaTypes();
	}
	
	private interface InternalFactory {
		Getter create(ScalarInputField source, Class<?> targetJavaType);
		Set<Class<?>> targetJavaTypes();
	}
	
	private static abstract class AbstractGetter implements Getter {
		private final ScalarInputField source;
		private final Class<?> targetJavaType;
		
		private AbstractGetter(ScalarInputField source, Class<?> targetJavaType) {
			this.source = source;
			this.targetJavaType = targetJavaType;
		}
		
		public final Object get() {
			if (source.isNull()) {
				return null;
			} else {
				return getNonNull(source, targetJavaType);
			}
		}
		
		protected abstract Object getNonNull(ScalarInputField source, Class<?> targetJavaType);
	}
	
	private static final class StringGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					StringInputField field = (StringInputField)source;
					return field.asString();
				}
			};
		}

		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(String.class);
			return supported;
		}
	}
	
	private static final class BinaryGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					BinaryInputField field = (BinaryInputField)source;
					byte[] values = field.asBinary();
					return ByteBuffer.wrap(values);
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(ByteBuffer.class);
			return supported;
		}
	}
	
	private static final class BooleanGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					BooleanInputField field = (BooleanInputField)source;
					return field.asBoolean();
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(Boolean.class);
			return supported;
		}
	}
	
	private static final class NumericGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					NumericInputField field = (NumericInputField)source;
					if (targetJavaType == BigInteger.class) {
						// TODO: exception if the conversion is not exact? 
						return field.asBigDecimal().toBigInteger();
					}
					return field.asBigDecimal();
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(BigInteger.class);
			supported.add(BigDecimal.class);
			return supported;
		}
	}
	
	private static final class DoubleGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					DoubleInputField field = (DoubleInputField)source;
					if (targetJavaType == BigDecimal.class) {
						return field.asBigDecimal();
					} 
					return field.asDouble();
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(BigDecimal.class);
			supported.add(Double.class);
			return supported;
		}
	}
	
	private static final class FloatGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					FloatInputField field = (FloatInputField)source;
					if (targetJavaType == BigDecimal.class) {
						return field.asBigDecimal();
					} else if (targetJavaType == Double.class) {
						return field.asDouble();
					} 
					return field.asFloat();
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(BigDecimal.class);
			supported.add(Double.class);
			supported.add(Float.class);
			return supported;
		}
	}
	
	private static final class LongGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					LongInputField field = (LongInputField)source;
					if (targetJavaType == BigDecimal.class) {
						return field.asBigDecimal();
					} else if (targetJavaType == BigInteger.class) {
						return field.asBigDecimal().toBigInteger();
					} else if (targetJavaType == Double.class) {
						// TODO: exception if the conversion is not exact? 
						return field.asDouble();
					} else if (targetJavaType == Float.class) {
						// TODO: exception if the conversion is not exact? 
						return field.asFloat();
					}
					return field.asLong();
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(BigDecimal.class);
			supported.add(BigInteger.class);
			supported.add(Double.class);
			supported.add(Float.class);
			supported.add(Long.class);
			return supported;
		}
	}
	
	private static final class IntegerGetterFactory implements InternalFactory {
		@Override
		public Getter create(ScalarInputField source, Class<?> targetJavaType) {
			return new AbstractGetter(source, targetJavaType) {
				@Override
				public Object getNonNull(ScalarInputField source, Class<?> targetJavaType) {
					IntInputField field = (IntInputField)source;
					if (targetJavaType == BigDecimal.class) {
						return field.asBigDecimal();
					} else if (targetJavaType == BigInteger.class) {
						return field.asBigDecimal().toBigInteger();
					} else if (targetJavaType == Double.class) {
						return field.asDouble();
					} else if (targetJavaType == Float.class) {
						return field.asFloat();
					} else if (targetJavaType == Long.class) {
						return field.asLong();
					} 
					return field.asInt();
				}
			};
		}
		
		@Override
		public Set<Class<?>> targetJavaTypes() {
			Set<Class<?>> supported = new HashSet<Class<?>>();
			supported.add(BigDecimal.class);
			supported.add(BigInteger.class);
			supported.add(Double.class);
			supported.add(Float.class);
			supported.add(Long.class);
			supported.add(Integer.class);
			return supported;
		}
	}
}
