package com.exasol.dynamodb.attributevalue;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * This class provides factory methods for quickly creating {@link AttributeValue}s.
 */
public class AttributeValueQuickCreator {

    public static AttributeValue forString(final String string) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setS(string);
        return attributeValue;
    }

    public static AttributeValue forNumber(final String string) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setN(string);
        return attributeValue;
    }

    public static AttributeValue forBinary(final ByteBuffer bytes) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setB(bytes);
        return attributeValue;
    }

    public static AttributeValue forNull() {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setNULL(true);
        return attributeValue;
    }

    public static AttributeValue forBoolean(final boolean value) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setBOOL(value);
        return attributeValue;
    }

    public static AttributeValue forList(final AttributeValue... values) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setL(List.of(values));
        return attributeValue;
    }

    public static AttributeValue forMap(final Map<String, AttributeValue> value) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setM(value);
        return attributeValue;
    }

    public static AttributeValue forBinarySet(final Collection<ByteBuffer> value) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setBS(value);
        return attributeValue;
    }

    public static AttributeValue forStringSet(final Collection<String> value) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setSS(value);
        return attributeValue;
    }

    public static AttributeValue forNumberSet(final Collection<String> value) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setNS(value);
        return attributeValue;
    }
}
