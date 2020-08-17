package com.exasol.adapter.document.mapping;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.sql.expression.ValueExpression;

class PropertyToVarcharColumnValueExtractorTest {

    private static final String TEST_STRING = "test";

    private static PropertyToVarcharColumnMapping.Builder getDefaultMappingBuilder() {
        return configureExampleMapping(PropertyToVarcharColumnMapping.builder())//
                .varcharColumnSize(TEST_STRING.length())//
                .overflowBehaviour(TruncateableMappingErrorBehaviour.ABORT);
    }

    @Test
    void testConvertStringRowBasic() {
        final PropertyToVarcharColumnMapping toStringColumnMappingDefinition = getDefaultMappingBuilder().build();
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        assertThat(exasolCellValue.toString(), equalTo(TEST_STRING));
    }

    @Test
    void testConvertRowOverflowTruncate() {
        final PropertyToVarcharColumnMapping toStringColumnMappingDefinition = getDefaultMappingBuilder()//
                .varcharColumnSize(TEST_STRING.length() - 1)//
                .overflowBehaviour(TruncateableMappingErrorBehaviour.TRUNCATE)//
                .build();
        final ValueExpression exasolCellValue = new ToStringValueMapperStub(toStringColumnMappingDefinition)
                .mapValue(null);
        final String expected = TEST_STRING.substring(0, TEST_STRING.length() - 1);
        assertThat(exasolCellValue.toString(), equalTo(expected));
    }

    @Test
    void testConvertRowOverflowException() {
        final PropertyToVarcharColumnMapping toStringColumnMappingDefinition = getDefaultMappingBuilder()//
                .varcharColumnSize(TEST_STRING.length() - 1)//
                .overflowBehaviour(TruncateableMappingErrorBehaviour.ABORT)//
                .build();
        final ToStringValueMapperStub valueMapper = new ToStringValueMapperStub(toStringColumnMappingDefinition);
        assertThrows(OverflowException.class, () -> valueMapper.mapValue(null));
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
