package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.sql.expression.ValueExpression;

class ToStringPropertyToColumnValueExtractorTest {

    private static final String TEST_STRING = "test";

    @Test
    void testConvertStringRowBasic() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping("",
                DocumentPathExpression.empty(), LookupFailBehaviour.DEFAULT_VALUE, TEST_STRING.length(),
                ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        assertThat(exasolCellValue.toString(), equalTo(TEST_STRING));
    }

    @Test
    void testConvertRowOverflowTruncate() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping("",
                DocumentPathExpression.empty(), LookupFailBehaviour.DEFAULT_VALUE, TEST_STRING.length() - 1,
                ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE);
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        final String expected = TEST_STRING.substring(0, TEST_STRING.length() - 1);
        assertThat(exasolCellValue.toString(), equalTo(expected));
    }

    @Test
    void testConvertRowOverflowException() {
        final ToStringPropertyToColumnMapping toStringColumnMappingDefinition = new ToStringPropertyToColumnMapping("",
                DocumentPathExpression.empty(), LookupFailBehaviour.DEFAULT_VALUE, TEST_STRING.length() - 1,
                ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
        assertThrows(OverflowException.class,
                () -> new ToStringValueMapperStub(toStringColumnMappingDefinition).mapValue(null));
    }

    private static class ToStringValueMapperStub extends ToStringPropertyToColumnValueExtractor<Void> {
        public ToStringValueMapperStub(final ToStringPropertyToColumnMapping column) {
            super(column);
        }

        @Override
        protected String mapStringValue(final DocumentNode<Void> dynamodbProperty) {
            return TEST_STRING;
        }
    }
}
