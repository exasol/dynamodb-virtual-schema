package com.exasol.adapter.dynamodb.literalconverter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.sql.*;

class SqlToDynamodbLiteralConverterTest {
    private static final SqlToDynamodbLiteralConverter CONVERTER = new SqlToDynamodbLiteralConverter();

    @Test
    void testConvertString() throws NotALiteralException {
        final String testValue = "test";
        final SqlNode literal = new SqlLiteralString(testValue);
        assertThat(CONVERTER.convert(literal).getS(), equalTo(testValue));
    }

    @Test
    void testConvertBoolean() throws NotALiteralException {
        final SqlNode literal = new SqlLiteralBool(true);
        assertThat(CONVERTER.convert(literal).getBOOL(), equalTo(true));
    }

    @Test
    void testConvertDouble() throws NotALiteralException {
        final SqlNode literal = new SqlLiteralDouble(0.1);
        assertThat(CONVERTER.convert(literal).getN(), equalTo("0.1"));
    }

    @Test
    void testConvertExactNumeric() throws NotALiteralException {
        final SqlNode literal = new SqlLiteralExactnumeric(new BigDecimal(10));
        assertThat(CONVERTER.convert(literal).getN(), equalTo("10"));
    }

    @Test
    void testConvertNull() throws NotALiteralException {
        final SqlNode literal = new SqlLiteralNull();
        assertThat(CONVERTER.convert(literal).getNULL(), equalTo(true));
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
        assertThrows(NotALiteralException.class, () -> CONVERTER.convert(nonLiteral));
    }
}