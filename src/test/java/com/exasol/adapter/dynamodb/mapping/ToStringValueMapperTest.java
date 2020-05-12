package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.sql.expression.ValueExpression;

public class ToStringValueMapperTest {

    private static final String TEST_STRING = "test";
    private static final AbstractPropertyToColumnMapping.ConstructorParameters COLUMN_PARAMETERS = new AbstractPropertyToColumnMapping.ConstructorParameters(
            "", new DocumentPathExpression.Builder().build(), LookupFailBehaviour.DEFAULT_VALUE);

    @Test
    void testConvertStringRowBasic() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping(
                COLUMN_PARAMETERS, TEST_STRING.length(), ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        assertThat(exasolCellValue.toString(), equalTo(TEST_STRING));
    }

    @Test
    void testConvertRowOverflowTruncate() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping(
                COLUMN_PARAMETERS, TEST_STRING.length() - 1,
                ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE);
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        final String expected = TEST_STRING.substring(0, TEST_STRING.length() - 1);
        assertThat(exasolCellValue.toString(), equalTo(expected));
    }

    @Test
    void testConvertRowOverflowException() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping(
                COLUMN_PARAMETERS, TEST_STRING.length() - 1,
                ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
        assertThrows(ToStringValueMapper.OverflowException.class,
                () -> new ToStringValueMapperStub(toStringColumnMappingDefinition).mapValue(null));
    }

    private static class ToStringValueMapperStub extends ToStringValueMapper<Void> {
        public ToStringValueMapperStub(final ToStringPropertyToColumnMapping column) {
            super(column);
        }

        @Override
        protected String mapStringValue(final DocumentNode<Void> dynamodbProperty) {
            return TEST_STRING;
        }
    }
}
