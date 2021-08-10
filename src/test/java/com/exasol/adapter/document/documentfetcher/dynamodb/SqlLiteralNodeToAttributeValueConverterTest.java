package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.sql.*;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class SqlLiteralNodeToAttributeValueConverterTest {

    private static final SqlLiteralNodeToAttributeValueConverter CONVERTER = new SqlLiteralNodeToAttributeValueConverter();

    @Test
    void testConvertString() {
        final AttributeValue result = CONVERTER.convert(new SqlLiteralString("test"));
        assertThat(result.s(), equalTo("test"));
    }

    @Test
    void testConvertDouble() {
        final AttributeValue result = CONVERTER.convert(new SqlLiteralDouble(1.23));
        assertThat(result.n(), equalTo("1.23"));
    }

    @Test
    void testConvertExactNumeric() {
        final AttributeValue result = CONVERTER.convert(new SqlLiteralExactnumeric(new BigDecimal("1.23")));
        assertThat(result.n(), equalTo("1.23"));
    }

    @Test
    void testConvertNull() {
        final AttributeValue result = CONVERTER.convert(new SqlLiteralNull());
        assertThat(result.nul(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testConvertBoolean(final boolean value) {
        final AttributeValue result = CONVERTER.convert(new SqlLiteralBool(value));
        assertThat(result.bool(), equalTo(value));
    }
}