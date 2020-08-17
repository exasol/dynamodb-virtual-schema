package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentValue;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbString;

class DynamodbAttributeValuePlaceholderMapBuilderTest {
    private static final DynamodbString VALUE_1 = new DynamodbString("1");
    private static final DynamodbString VALUE_2 = new DynamodbString("2");

    @Test
    void testBuild() {
        final DynamodbAttributeValuePlaceholderMapBuilder builder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String key1 = builder.addValue(VALUE_1);
        final String key2 = builder.addValue(VALUE_2);
        final Map<String, DocumentValue<DynamodbNodeVisitor>> valueMap = builder.getPlaceholderMap();
        assertThat(valueMap.get(key1), equalTo(VALUE_1));
        assertThat(valueMap.get(key2), equalTo(VALUE_2));
    }
}