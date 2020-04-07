package com.exasol.dynamodb.attributevalue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Visitor interface for {@link AttributeValue}. As {@link AttributeValue} does not support this Visitor natively use
 * {@link AttributeValueWrapper}
 */
public interface AttributeValueVisitor {

    /**
     * Called when AttributeValue has a string value
     * 
     * @param value string value
     */
    public default void visitString(final String value) {
        throw new UnsupportedDynamodbTypeException("String");
    }

    /**
     * Called when AttributeValue has a number value
     * 
     * @param value string containing number value
     */
    public default void visitNumber(final String value) {
        throw new UnsupportedDynamodbTypeException("Number");
    }

    /**
     * Called when AttributeValue has a binary value
     * 
     * @param value byte value
     */
    public default void visitBinary(final ByteBuffer value) {
        throw new UnsupportedDynamodbTypeException("Binary");
    }

    /**
     * Called when AttributeValue has a boolean value
     * 
     * @param value byte value
     */
    public default void visitBoolean(final boolean value) {
        throw new UnsupportedDynamodbTypeException("Boolean");
    }

    /**
     * Called when AttributeValue has a map value
     * 
     * @param value map value
     */
    public default void visitMap(final Map<String, AttributeValue> value) {
        throw new UnsupportedDynamodbTypeException("Map");
    }

    /**
     * Called when AttributeValue has a ByteSet value
     *
     * @param value ByteSet value
     */
    public default void visitByteSet(final List<ByteBuffer> value) {
        throw new UnsupportedDynamodbTypeException("ByteSet");
    }

    /**
     * Called when AttributeValue has a list
     *
     * @param value list value
     */
    public default void visitList(final List<AttributeValue> value) {
        throw new UnsupportedDynamodbTypeException("List");
    }

    /**
     * Called when AttributeValue has a NumberSet
     *
     * @param value NumberSet value
     */
    public default void visitNumberSet(final List<String> value) {
        throw new UnsupportedDynamodbTypeException("NumberSet");
    }

    /**
     * Called when AttributeValue has a StringSet
     *
     * @param value StringSet value
     */
    public default void visitStringSet(final List<String> value) {
        throw new UnsupportedDynamodbTypeException("StringSet");
    }

    /**
     * Called when AttributeValue is NULL
     */
    public default void visitNull() {
        throw new UnsupportedDynamodbTypeException("Null");
    }

}
