package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.sql.SqlLiteralString;
import com.exasol.adapter.sql.SqlNode;

class DynamodbAttributeValuePlaceholderMapBuilderTest {
    private static final SqlLiteralString VALUE_1 = new SqlLiteralString("1");
    private static final SqlLiteralString VALUE_2 = new SqlLiteralString("2");

    @Test
    void testBuild() {
        final DynamodbAttributeValuePlaceholderMapBuilder builder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String key1 = builder.addValue(VALUE_1);
        final String key2 = builder.addValue(VALUE_2);
        final Map<String, SqlNode> valueMap = builder.getPlaceholderMap();
        assertThat(valueMap.get(key1), equalTo(VALUE_1));
        assertThat(valueMap.get(key2), equalTo(VALUE_2));
    }
}