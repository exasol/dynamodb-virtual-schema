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
     * Called when AttributeValue has a string value.
     * 
     * @param value string value
     */
    public default void visitString(final String value) {
        defaultVisit("String");
    }

    /**
     * Called when AttributeValue has a number value.
     * 
     * @param value string containing number value
     */
    public default void visitNumber(final String value) {
        defaultVisit("Number");
    }

    /**
     * Called when AttributeValue has a binary value.
     * 
     * @param value byte value
     */
    public default void visitBinary(final ByteBuffer value) {
        defaultVisit("Binary");
    }

    /**
     * Called when AttributeValue has a boolean value.
     * 
     * @param value byte value
     */
    public default void visitBoolean(final boolean value) {
        defaultVisit("Boolean");
    }

    /**
     * Called when AttributeValue has a map value.
     * 
     * @param value map value
     */
    public default void visitMap(final Map<String, AttributeValue> value) {
        defaultVisit("Map");
    }

    /**
     * Called when AttributeValue has a ByteSet value.
     *
     * @param value ByteSet value
     */
    public default void visitByteSet(final List<ByteBuffer> value) {
        defaultVisit("ByteSet");
    }

    /**
     * Called when AttributeValue has a list value.
     *
     * @param value list value
     */
    public default void visitList(final List<AttributeValue> value) {
        defaultVisit("List");
    }

    /**
     * Called when AttributeValue has a NumberSet value.
     *
     * @param value NumberSet value
     */
    public default void visitNumberSet(final List<String> value) {
        defaultVisit("NumberSet");
    }

    /**
     * Called when AttributeValue has a StringSet value.
     *
     * @param value StringSet value
     */
    public default void visitStringSet(final List<String> value) {
        defaultVisit("StringSet");
    }

    /**
     * Called when AttributeValue is NULL value.
     */
    public default void visitNull() {
        defaultVisit("Null");
    }

    /**
     * Called when the specific visit method was not implemented. This method can for example be used for throwing an
     * {@link UnsupportedOperationException}.
     * 
     * @param typeName name of the DynamoDB type to visit.
     */
    public void defaultVisit(final String typeName);

}
