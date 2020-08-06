package com.exasol.dynamodb.attributevalue;

import java.util.Collection;
import java.util.Map;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class provides factory methods for quickly creating {@link AttributeValue}s.
 */
public class AttributeValueQuickCreator {

    public static AttributeValue forString(final String string) {
        return AttributeValue.builder().s(string).build();
    }

    public static AttributeValue forNumber(final String string) {
        return AttributeValue.builder().n(string).build();
    }

    public static AttributeValue forBinary(final SdkBytes bytes) {
        return AttributeValue.builder().b(bytes).build();
    }

    public static AttributeValue forNull() {
        return AttributeValue.builder().nul(true).build();
    }

    public static AttributeValue forBoolean(final boolean value) {
        return AttributeValue.builder().bool(value).build();
    }

    public static AttributeValue forList(final AttributeValue... values) {
        return AttributeValue.builder().l(values).build();
    }

    public static AttributeValue forMap(final Map<String, AttributeValue> value) {
        return AttributeValue.builder().m(value).build();
    }

    public static AttributeValue forBinarySet(final Collection<SdkBytes> value) {
        return AttributeValue.builder().bs(value).build();
    }

    public static AttributeValue forStringSet(final Collection<String> value) {
        return AttributeValue.builder().ss(value).build();
    }

    public static AttributeValue forNumberSet(final Collection<String> value) {
        return AttributeValue.builder().ns(value).build();
    }
}
