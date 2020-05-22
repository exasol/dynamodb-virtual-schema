package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

class DynamodbAttributeValuePlaceholderMapBuilderTest {
    private static final AttributeValue VALUE_1 = AttributeValueQuickCreator.forString("1");
    private static final AttributeValue VALUE_2 = AttributeValueQuickCreator.forString("2");

    @Test
    void testBuild() {
        final DynamodbAttributeValuePlaceholderMapBuilder builder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String key1 = builder.addValue(VALUE_1);
        final String key2 = builder.addValue(VALUE_2);
        final Map<String, AttributeValue> valueMap = builder.getPlaceholderMap();
        assertThat(valueMap.get(key1), equalTo(VALUE_1));
        assertThat(valueMap.get(key2), equalTo(VALUE_2));
    }
}