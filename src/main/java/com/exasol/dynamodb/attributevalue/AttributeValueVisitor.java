package com.exasol.dynamodb.attributevalue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Visitor interface for {@link AttributeValue}. As {@link AttributeValue} does
 * not support this Visitor natively use {@link AttributeValueWrapper}
 */
public interface AttributeValueVisitor {

	/**
	 * Called when AttributeValue has a string value
	 * 
	 * @param value
	 *            string value
	 */
	default void visitString(final String value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a number value
	 * 
	 * @param value
	 *            string containing number value
	 */
	default void visitNumber(final String value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a binary value
	 * 
	 * @param value
	 *            byte value
	 */
	default void visitBinary(final ByteBuffer value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a boolean value
	 * 
	 * @param value
	 *            byte value
	 */
	default void visitBoolean(final boolean value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a map value
	 * 
	 * @param value
	 *            map value
	 */
	default void visitMap(final Map<String, AttributeValue> value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a ByteSet value
	 *
	 * @param value
	 *            ByteSet value
	 */
	default void visitByteSet(final List<ByteBuffer> value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a list
	 *
	 * @param value
	 *            list value
	 */
	default void visitList(final List<AttributeValue> value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a NumberSet
	 *
	 * @param value
	 *            NumberSet value
	 */
	default void visitNumberSet(final List<String> value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue has a StringSet
	 *
	 * @param value
	 *            StringSet value
	 */
	default void visitStringSet(final List<String> value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * Called when AttributeValue is NULL
	 */
	default void visitNull() {
		throw new UnsupportedOperationException("not yet implemented");
	}
}
