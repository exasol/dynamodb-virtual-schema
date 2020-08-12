package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.sql.expression.ValueExpression;

class PropertyToVarcharColumnValueExtractorTest {

    private static final String TEST_STRING = "test";

    @Test
    void testConvertStringRowBasic() {
        final PropertyToVarcharColumnMapping toStringColumnMappingDefinition = new PropertyToVarcharColumnMapping("",
                DocumentPathExpression.empty(), MappingErrorBehaviour.NULL, TEST_STRING.length(),
                TruncateableMappingErrorBehaviour.ABORT);
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        assertThat(exasolCellValue.toString(), equalTo(TEST_STRING));
    }

    @Test
    void testConvertRowOverflowTruncate() {
        final PropertyToVarcharColumnMapping toStringColumnMappingDefinition = new PropertyToVarcharColumnMapping("",
                DocumentPathExpression.empty(), MappingErrorBehaviour.NULL, TEST_STRING.length() - 1,
                TruncateableMappingErrorBehaviour.TRUNCATE);
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        final String expected = TEST_STRING.substring(0, TEST_STRING.length() - 1);
        assertThat(exasolCellValue.toString(), equalTo(expected));
    }

    @Test
    void testConvertRowOverflowException() {
        final PropertyToVarcharColumnMapping toStringColumnMappingDefinition = new PropertyToVarcharColumnMapping("",
                DocumentPathExpression.empty(), MappingErrorBehaviour.NULL, TEST_STRING.length() - 1,
                TruncateableMappingErrorBehaviour.ABORT);
        final ToStringValueMapperStub valueMapper = new ToStringValueMapperStub(toStringColumnMappingDefinition);
        assertThrows(OverflowException.class,
                () -> valueMapper.mapValue(null));
    }

    private static class ToStringValueMapperStub extends PropertyToVarcharColumnValueExtractor<Void> {
        public ToStringValueMapperStub(final PropertyToVarcharColumnMapping column) {
            super(column);
        }

        @Override
        protected String mapStringValue(final DocumentNode<Void> dynamodbProperty) {
            return TEST_STRING;
        }
    }
}
