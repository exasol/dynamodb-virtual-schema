package com.exasol.dynamodb.attributevalue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class AttributeValueWrapperTest {

	@Test
	void testStringValue() {
		final String testString = "testString";
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setS(testString);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			String stringValue;
			@Override
			public void visitString(final String value) {
				this.stringValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.stringValue, equalTo(testString));
	}

	@Test
	void testStringValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setS("");
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testNumberValue() {
		final String testNumber = "123";
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setN(testNumber);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			String numberValue;

			@Override
			public void visitNumber(final String value) {
				this.numberValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.numberValue, equalTo(testNumber));
	}

	@Test
	void testNumberValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setN("");
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testBinaryValue() {
		final ByteBuffer bytes = ByteBuffer.wrap("test".getBytes());
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setB(bytes);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			ByteBuffer byteValue;

			@Override
			public void visitBinary(final ByteBuffer value) {
				this.byteValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.byteValue, equalTo(bytes));
	}

	@Test
	void testBinaryValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setB(ByteBuffer.wrap("test".getBytes()));
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testBooleanValue() {
		final boolean testValue = true;
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setBOOL(testValue);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			boolean booleanValue;
			@Override
			public void visitBoolean(final boolean value) {
				this.booleanValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.booleanValue, equalTo(testValue));
	}

	@Test
	void testBooleanValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setBOOL(true);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testMapValue() {
		final Map<String, AttributeValue> testMap = Map.of("key", new AttributeValue());
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setM(testMap);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			Map<String, AttributeValue> mapValue;

			@Override
			public void visitMap(final Map<String, AttributeValue> value) {
				this.mapValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.mapValue, equalTo(testMap));
	}

	@Test
	void testMapValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setM(Map.of());
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testByteSetValue() {
		final List<ByteBuffer> testByteSet = List.of(ByteBuffer.wrap("test".getBytes()));
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setBS(testByteSet);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			List<ByteBuffer> byteSetValue;

			@Override
			public void visitByteSet(final List<ByteBuffer> value) {
				this.byteSetValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.byteSetValue, equalTo(testByteSet));
	}

	@Test
	void testByteSetValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setBS(List.of());
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testListValue() {
		final List<AttributeValue> testList = List.of(new AttributeValue());
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setL(testList);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			List<AttributeValue> listValue;

			@Override
			public void visitList(final List<AttributeValue> value) {
				this.listValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.listValue, equalTo(testList));
	}

	@Test
	void testListValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setL(List.of());
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testNumberSetValue() {
		final List<String> testNumberSet = List.of("123");
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setNS(testNumberSet);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			List<String> numberSetValue;

			@Override
			public void visitNumberSet(final List<String> value) {
				this.numberSetValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.numberSetValue, equalTo(testNumberSet));
	}

	@Test
	void testNumberSetValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setNS(List.of("123"));
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testStringSetValue() {
		final List<String> testStringSet = List.of("test");
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setSS(testStringSet);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			List<String> stringSetValue;

			@Override
			public void visitStringSet(final List<String> value) {
				this.stringSetValue = value;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.stringSetValue, equalTo(testStringSet));
	}

	@Test
	void testStringSetValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setSS(List.of("test"));
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testNullValue() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setNULL(true);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		class TestVisitor implements AttributeValueVisitor {
			boolean wasCalled = false;

			@Override
			public void visitNull() {
				this.wasCalled = true;
			}
		}
		final TestVisitor testVisitor = new TestVisitor();
		attributeValueWrapper.accept(testVisitor);
		assertThat(testVisitor.wasCalled, equalTo(true));
	}

	@Test
	void testNullValueUnsupportedOperation() {
		final AttributeValue attributeValue = new AttributeValue();
		attributeValue.setNULL(true);
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(AttributeValueVisitor.UnsupportedDynamodbTypeException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}

	@Test
	void testUnsupportedType() {
		final AttributeValue attributeValue = new AttributeValue();
		final AttributeValueWrapper attributeValueWrapper = new AttributeValueWrapper(attributeValue);
		assertThrows(UnsupportedOperationException.class,
				() -> attributeValueWrapper.accept(new AttributeValueVisitor() {
				}));
	}
}
