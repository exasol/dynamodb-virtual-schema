package com.exasol.adapter.dynamodb.literalconverter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbBoolean;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNull;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNumber;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.literalconverter.NotLiteralException;
import com.exasol.adapter.sql.*;

class SqlLiteralToDynamodbValueConverterTest {
    private static final SqlLiteralToDynamodbValueConverter CONVERTER = new SqlLiteralToDynamodbValueConverter();

    @Test
    void testConvertString() throws NotLiteralException {
        final String testValue = "test";
        final SqlNode literal = new SqlLiteralString(testValue);
        final DynamodbString result = (DynamodbString) CONVERTER.convert(literal);
        assertThat(result.getValue(), equalTo(testValue));
    }

    @Test
    void testConvertBoolean() throws NotLiteralException {
        final SqlNode literal = new SqlLiteralBool(true);
        final DynamodbBoolean result = (DynamodbBoolean) CONVERTER.convert(literal);
        assertThat(result.getValue(), equalTo(true));
    }

    @Test
    void testConvertDouble() throws NotLiteralException {
        final SqlNode literal = new SqlLiteralDouble(0.1);
        final DynamodbNumber result = (DynamodbNumber) CONVERTER.convert(literal);
        assertThat(result.getValue(), equalTo("0.1"));
    }

    @Test
    void testConvertExactNumeric() throws NotLiteralException {
        final SqlNode literal = new SqlLiteralExactnumeric(new BigDecimal(10));
        final DynamodbNumber result = (DynamodbNumber) CONVERTER.convert(literal);
        assertThat(result.getValue(), equalTo("10"));
    }

    @Test
    void testConvertNull() throws NotLiteralException {
        final SqlNode literal = new SqlLiteralNull();
        assertThat(CONVERTER.convert(literal), instanceOf(DynamodbNull.class));
    }

    @Test
    void testUnsupported() {
        final List<SqlNode> unsupportedLiterals = List.of(new SqlLiteralDate(null), new SqlLiteralTimestamp(null),
                new SqlLiteralTimestampUtc(null), new SqlLiteralInterval(null, null));
        for (final SqlNode unsupportedLiteral : unsupportedLiterals) {
            final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                    () -> CONVERTER.convert(unsupportedLiteral));
            assertThat(exception.getMessage(), startsWith("DynamoDB has no corresponding literal for Exasol's"));
        }
    }

    @Test
    void testNonLiteral() {
        final SqlNode nonLiteral = new SqlPredicateAnd(List.of());
        assertThrows(NotLiteralException.class, () -> CONVERTER.convert(nonLiteral));
    }
}