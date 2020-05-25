package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.*;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

class ValueExpressionsToSqlSelectFromValuesConverterTest {
    SchemaMappingQuery getRemoteTableQueryStub() {
        return new SchemaMappingQuery() {
            @Override
            public TableMapping getFromTable() {
                return null;
            }

            @Override
            public List<ColumnMapping> getSelectList() {
                return List.of(new ToJsonPropertyToColumnMapping("json", DocumentPathExpression.empty(),
                        LookupFailBehaviour.DEFAULT_VALUE));
            }
        };
    }

    @Test
    void testEmptyConvert() {
        final ValueExpressionsToSqlSelectFromValuesConverter converter = new ValueExpressionsToSqlSelectFromValuesConverter();
        final String sql = converter.convert(getRemoteTableQueryStub(), Collections.emptyList());
        assertThat(sql, equalTo("SELECT * FROM (VALUES ('')) WHERE FALSE"));
    }

    @Test
    void testSingleItemConvert() {
        final String testString = "test";
        final ValueExpression stringFrame = StringLiteral.of(testString);
        final ValueExpressionsToSqlSelectFromValuesConverter converter = new ValueExpressionsToSqlSelectFromValuesConverter();
        final String sql = converter.convert(getRemoteTableQueryStub(), List.of(List.of(stringFrame)));
        assertThat(sql, equalTo("SELECT * FROM (VALUES ('" + testString + "'))"));
    }

    @Test
    void testTwoItemConvert() {
        final String testString1 = "test1";
        final ValueExpression stringFrame1 = StringLiteral.of(testString1);
        final String testString2 = "test2";
        final ValueExpression stringFrame2 = StringLiteral.of(testString2);

        final ValueExpressionsToSqlSelectFromValuesConverter converter = new ValueExpressionsToSqlSelectFromValuesConverter();
        final String sql = converter.convert(getRemoteTableQueryStub(),
                List.of(List.of(stringFrame1), List.of(stringFrame2)));
        assertThat(sql, equalTo("SELECT * FROM (VALUES ('" + testString1 + "'), ('" + testString2 + "'))"));
    }
}
