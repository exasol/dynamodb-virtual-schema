package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueTestUtils;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalkerException;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

public class AbstractValueMapperTest {
    @Test
    void testLookup() {
        final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("isbn", null);
        final MockColumnMappingDefinition columnMappingDefinition = new MockColumnMappingDefinition("d", resultWalker,
                AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
        final String isbn = "123456789";
        final AttributeValue isbnValue = AttributeValueTestUtils.forString(isbn);
        final ValueExpression valueExpression = new MockValueMapper(columnMappingDefinition)
                .mapRow(Map.of("isbn", isbnValue));
        assertThat(valueExpression.toString(), equalTo(isbn));
    }

    @Test
    void testNullLookupFailBehaviour() throws ValueMapperException, DynamodbResultWalkerException {
        final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
        final MockColumnMappingDefinition columnMappingDefinition = new MockColumnMappingDefinition("d", resultWalker,
                AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE);
        final ValueExpression valueExpression = new MockValueMapper(columnMappingDefinition)
                .mapRow(Collections.emptyMap());
        assertThat(valueExpression.toString(), equalTo("default"));
    }

    @Test
    void testExceptionLookupFailBehaviour() {
        final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
        final MockColumnMappingDefinition columnMappingDefinition = new MockColumnMappingDefinition("d", resultWalker,
                AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
        assertThrows(DynamodbResultWalkerException.class,
                () -> new MockValueMapper(columnMappingDefinition).mapRow(Collections.emptyMap()));
    }

    @Test
    public void testColumnMappingException() {
        final String columnName = "name";
        final MockColumnMappingDefinition mappingDefinition = new MockColumnMappingDefinition(columnName,
                new IdentityDynamodbResultWalker(), AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
        final ValueMapperException exception = assertThrows(ValueMapperException.class,
                () -> new ExceptionMockValueMapper(mappingDefinition).mapRow(Map.of()));
        assertThat(exception.getCausingColumn().getExasolColumnName(), equalTo(columnName));
    }

    private static class MockValueMapper extends AbstractValueMapper {

        public MockValueMapper(final AbstractColumnMappingDefinition column) {
            super(column);
        }

        @Override
        protected ValueExpression mapValue(final AttributeValue dynamodbProperty) {
            return StringLiteral.of(dynamodbProperty.getS());
        }
    }

    private static class ExceptionMockValueMapper extends AbstractValueMapper {
        private final AbstractColumnMappingDefinition column;

        public ExceptionMockValueMapper(final AbstractColumnMappingDefinition column) {
            super(column);
            this.column = column;
        }

        @Override
        protected ValueExpression mapValue(final AttributeValue dynamodbProperty) throws ValueMapperException {
            throw new ValueMapperException("mocMessage", this.column);
        }
    }
}
