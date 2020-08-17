package com.exasol.adapter.document.mapping;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.sql.expression.BigDecimalLiteral;
import com.exasol.sql.expression.NullLiteral;
import com.exasol.sql.expression.ValueExpression;

class PropertyToDecimalColumnValueExtractorTest {

    private static final PropertyToDecimalColumnMapping ABORT_MAPPING = commonMappingBuilder()//
            .overflowBehaviour(MappingErrorBehaviour.ABORT)//
            .notNumericBehaviour(MappingErrorBehaviour.ABORT)//
            .build();
    private static final PropertyToDecimalColumnMapping NULL_MAPPING = commonMappingBuilder()//
            .overflowBehaviour(MappingErrorBehaviour.NULL)//
            .notNumericBehaviour(MappingErrorBehaviour.NULL)//
            .build();

    private static PropertyToDecimalColumnMapping.Builder commonMappingBuilder() {
        return configureExampleMapping(PropertyToDecimalColumnMapping.builder())//
                .decimalPrecision(2)//
                .decimalScale(0);
    }

    @Test
    void testConvert() {
        final BigDecimal bigDecimalValue = BigDecimal.valueOf(10);
        final ToDecimalExtractorStub extractor = new ToDecimalExtractorStub(ABORT_MAPPING, bigDecimalValue);
        final BigDecimalLiteral result = (BigDecimalLiteral) extractor.mapValue(null);
        assertThat(result.getValue(), equalTo(BigDecimal.valueOf(10)));
    }

    @Test
    void testOverflowException() {
        final BigDecimal bigDecimalValue = BigDecimal.valueOf(100);
        final ToDecimalExtractorStub extractor = new ToDecimalExtractorStub(ABORT_MAPPING, bigDecimalValue);
        final OverflowException exception = assertThrows(OverflowException.class, () -> extractor.mapValue(null));
        assertThat(exception.getMessage(), equalTo(
                "The input value exceeded the size of the EXASOL_COLUMN DECIMAL column. You can either increase the DECIMAL precision of this column or set the overflow behaviour to NULL."));
    }

    @Test
    void testOverflowNull() {
        final ToDecimalExtractorStub extractor = new ToDecimalExtractorStub(NULL_MAPPING, BigDecimal.valueOf(100));
        final ValueExpression valueExpression = extractor.mapValue(null);
        assertThat(valueExpression, instanceOf(NullLiteral.class));
    }

    @Test
    void testNaNHandlingException() {
        final ToDecimalExtractorStub extractor = new ToDecimalExtractorStub(ABORT_MAPPING, null);
        final ColumnValueExtractorException exception = assertThrows(ColumnValueExtractorException.class,
                () -> extractor.mapValue(null));
        assertThat(exception.getMessage(), equalTo(
                "The input value was no number. Try using a different mapping or ignore this error by setting notNumericBehaviour = \"null\"."));
    }

    @Test
    void testNaNHandlingNull() {
        final ToDecimalExtractorStub extractor = new ToDecimalExtractorStub(NULL_MAPPING, null);
        final ValueExpression valueExpression = extractor.mapValue(null);
        assertThat(valueExpression, instanceOf(NullLiteral.class));
    }

    private static class ToDecimalExtractorStub extends PropertyToDecimalColumnValueExtractor<Object> {
        private final BigDecimal value;

        public ToDecimalExtractorStub(final PropertyToDecimalColumnMapping column, final BigDecimal value) {
            super(column);
            this.value = value;
        }

        @Override
        protected BigDecimal mapValueToDecimal(final DocumentNode<Object> documentValue) {
            return this.value;
        }
    }
}