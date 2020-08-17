package com.exasol.adapter.document.literalconverter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.sql.*;
import com.exasol.sql.expression.*;

class SqlLiteralToValueExpressionConverterTest {

    private static final SqlLiteralToValueExpressionConverter CONVERTER = SqlLiteralToValueExpressionConverter
            .getInstance();

    @Test
    void testConvertString() {
        final String testString = "test";
        final StringLiteral result = (StringLiteral) CONVERTER.convert(new SqlLiteralString(testString));
        assertThat(result.toString(), equalTo(testString));
    }

    @Test
    void testConvertLong() {
        final long number = 1346464;
        final LongLiteral result = (LongLiteral) CONVERTER
                .convert(new SqlLiteralExactnumeric(BigDecimal.valueOf(number)));
        assertThat(result.getValue(), equalTo(number));
    }

    @Test
    void testConvertExactnummericDouble() {
        final double number = 12.456;
        final DoubleLiteral result = (DoubleLiteral) CONVERTER
                .convert(new SqlLiteralExactnumeric(BigDecimal.valueOf(number)));
        assertThat(result.getValue(), equalTo(number));
    }

    @Test
    void testConvertDouble() {
        final double number = 12.456;
        final DoubleLiteral result = (DoubleLiteral) CONVERTER.convert(new SqlLiteralDouble(number));
        assertThat(result.getValue(), equalTo(number));
    }

    @Test
    void testConvertTrue() {
        final BooleanLiteral result = (BooleanLiteral) CONVERTER.convert(new SqlLiteralBool(true));
        assertThat(result.toBoolean(), equalTo(true));
    }

    @Test
    void testConvertFalse() {
        final BooleanLiteral result = (BooleanLiteral) CONVERTER.convert(new SqlLiteralBool(false));
        assertThat(result.toBoolean(), equalTo(false));
    }

    @Test
    void testConvertNull() {
        final ValueExpression result = CONVERTER.convert(new SqlLiteralNull());
        assertThat(result, instanceOf(NullLiteral.class));
    }

}