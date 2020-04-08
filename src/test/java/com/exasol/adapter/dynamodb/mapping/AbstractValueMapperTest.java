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

/**
 * Tests for {@link AbstractValueMapper}
 */
public class AbstractValueMapperTest {
    @Test
    void testLookup() throws DynamodbResultWalkerException, ValueMapperException {
        final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("isbn", null);
        final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
                AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
        final String isbn = "123456789";
        final AttributeValue isbnValue = AttributeValueTestUtils.forString(isbn);
        final ValueExpression valueExpression = new MocValueMapper(columnMappingDefinition)
                .mapRow(Map.of("isbn", isbnValue));
        assertThat(valueExpression.toString(), equalTo(isbn));
    }

    @Test
    void testNullLookupFailBehaviour() throws ValueMapperException, DynamodbResultWalkerException {
        final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
        final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
                AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE);
        final ValueExpression valueExpression = new MocValueMapper(columnMappingDefinition)
                .mapRow(Collections.emptyMap());
        assertThat(valueExpression.toString(), equalTo("default"));
    }

    @Test
    void testExceptionLookupFailBehaviour() {
        final ObjectDynamodbResultWalker resultWalker = new ObjectDynamodbResultWalker("nonExistingColumn", null);
        final MocColumnMappingDefinition columnMappingDefinition = new MocColumnMappingDefinition("d", resultWalker,
                AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
        assertThrows(DynamodbResultWalkerException.class,
                () -> new MocValueMapper(columnMappingDefinition).mapRow(Collections.emptyMap()));
    }

    @Test
    public void testColumnMappingException() {
        final String columnName = "name";
        final MocColumnMappingDefinition mappingDefinition = new MocColumnMappingDefinition(columnName,
                new IdentityDynamodbResultWalker(), AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION);
        final ValueMapperException exception = assertThrows(ValueMapperException.class,
                () -> new ExceptionMocValueMapper(mappingDefinition).mapRow(Map.of()));
        assertThat(exception.getCausingColumn().getExasolName(), equalTo(columnName));
    }

    private static class MocValueMapper extends AbstractValueMapper {

        public MocValueMapper(final AbstractColumnMappingDefinition column) {
            super(column);
        }

        @Override
        protected ValueExpression mapValue(final AttributeValue dynamodbProperty) throws ValueMapperException {
            return StringLiteral.of(dynamodbProperty.getS());
        }
    }

    private static class ExceptionMocValueMapper extends AbstractValueMapper {
        AbstractColumnMappingDefinition column;

        public ExceptionMocValueMapper(final AbstractColumnMappingDefinition column) {
            super(column);
            this.column = column;
        }

        @Override
        protected ValueExpression mapValue(final AttributeValue dynamodbProperty) throws ValueMapperException {
            throw new ValueMapperException("mocMessage", this.column);
        }
    }
}
