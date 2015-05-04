package com.actian.ilabs.dataflow.cassandra.setter;

import com.datastax.driver.core.Row;
import com.pervasive.datarush.ports.physical.ScalarOutputField;
import com.pervasive.datarush.tokens.scalar.*;
import com.pervasive.datarush.types.ScalarTokenType;
import com.pervasive.datarush.types.TokenTypeConstant;
//import net.sf.antcontrib.inifile.IniFileTask.Set;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class SetterFactory {

	private static final Map<Class<?>, InternalFactory> factories;
	static {
		Map<Class<?>, InternalFactory> map = new HashMap<Class<?>, InternalFactory>();
		map.put(String.class, new StringSetterFactory());
		map.put(ByteBuffer.class, new ByteBufferSetterFactory());
		
		map.put(Boolean.class, new BooleanSetterFactory());
		
		map.put(BigDecimal.class, new BigDecimalSetterFactory());
		map.put(BigInteger.class, new BigIntegerSetterFactory());
		map.put(Double.class, new DoubleSetterFactory());
		map.put(Float.class, new FloatSetterFactory());
		map.put(Long.class, new LongSetterFactory());
		map.put(Integer.class, new IntegerSetterFactory());
		
		map.put(Date.class, new DateSetterFactory());
		map.put(InetAddress.class, new InetAdressSetterFactory());
		map.put(UUID.class, new UUIDSetterFactory());
		
		map.put(List.class, new ListSetterFactory());
		map.put(Set.class, new SetSetterFactory());
		map.put(Map.class, new MapSetterFactory());
		factories = Collections.unmodifiableMap(map);
	}
	
	public static ScalarTokenType targetTokenType(Class<?> sourceType) {
		return factories.get(sourceType).targetType();
	}
	
	public static Setter setter(ScalarOutputField target, Class<?> sourceJavaType) {
		return factories.get(sourceJavaType).create(target);
	}
	
	private static interface InternalFactory {
		public Setter create(ScalarOutputField target);
		public ScalarTokenType targetType();
	}
	
	private static abstract class AbstractSetter implements Setter {
		protected final ScalarOutputField target;
		
		private AbstractSetter(ScalarOutputField target) {
			this.target = target;
		}
		
		public final void set(Row row) {
			if (row.isNull(target.getName())) {
				target.setNull();
			} else {
				setNonNull(row);
			}
		}
		
		protected abstract void setNonNull(Row row);
	}
	
	private static final class StringSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					StringSettable settable = (StringSettable)target;
					String value = row.getString(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.STRING;
		}
	}
	
	private static final class ByteBufferSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					BinarySettable settable = (BinarySettable)target;
					byte[] value = row.getBytes(target.getName()).array();
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.BINARY;
		}
	}
	
	private static final class BooleanSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					BooleanSettable settable = (BooleanSettable)target;
					boolean value = row.getBool(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.BOOLEAN;
		}
	}
	
	private static final class BigDecimalSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					NumericSettable settable = (NumericSettable)target;
					BigDecimal value = row.getDecimal(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.NUMERIC;
		}
	}
	
	private static final class BigIntegerSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					NumericSettable settable = (NumericSettable)target;
					BigInteger value = row.getVarint(target.getName());
					settable.set(new BigDecimal(value));
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.NUMERIC;
		}
	}
	
	private static final class DoubleSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					DoubleSettable settable = (DoubleSettable)target;
					double value = row.getDouble(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.DOUBLE;
		}
	}
	
	private static final class FloatSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					FloatSettable settable = (FloatSettable)target;
					float value = row.getFloat(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.FLOAT;
		}
	}
	
	private static final class LongSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					LongSettable settable = (LongSettable)target;
					long value = row.getLong(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.LONG;
		}
	}
	
	private static final class IntegerSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					IntSettable settable = (IntSettable)target;
					int value = row.getInt(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.INT;
		}
	}
	
	private static final class DateSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					DateSettable settable = (DateSettable)target;
					Date value = row.getDate(target.getName());
					settable.set(value.getTime());
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.DATE;
		}
	}
	
	private static final class InetAdressSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					@SuppressWarnings("unchecked")
					ObjectSettable<Object> settable = (ObjectSettable<Object>)target;
					Object value = row.getInet(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.OBJECT(Object.class);
		}
	}
	
	private static final class UUIDSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					@SuppressWarnings("unchecked")
					ObjectSettable<Object> settable = (ObjectSettable<Object>)target;
					Object value = row.getUUID(target.getName());
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.OBJECT(Object.class);
		}
	}
	
	private static final class ListSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					@SuppressWarnings("unchecked")
					ObjectSettable<Object> settable = (ObjectSettable<Object>)target;
					Object value = row.getList(target.getName(), Object.class);
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.OBJECT(Object.class);
		}
	}
	
	private static final class SetSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					@SuppressWarnings("unchecked")
					ObjectSettable<Object> settable = (ObjectSettable<Object>)target;
					Object value = row.getSet(target.getName(), Object.class);
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.OBJECT(Object.class);
		}
	}
	
	private static final class MapSetterFactory implements InternalFactory {
		@Override
		public Setter create(ScalarOutputField target) {
			return new AbstractSetter(target) {
				@Override
				public void setNonNull(Row row) {
					@SuppressWarnings("unchecked")
					ObjectSettable<Object> settable = (ObjectSettable<Object>)target;
					Object value = row.getMap(target.getName(), Object.class, Object.class);
					settable.set(value);
				}
			};
		}

		@Override
		public ScalarTokenType targetType() {
			return TokenTypeConstant.OBJECT(Object.class);
		}
	}
}
