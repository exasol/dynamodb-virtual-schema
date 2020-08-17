package com.exasol.adapter.document.mapping.dynamodb;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.document.documentpath.StaticDocumentPathIterator;
import com.exasol.adapter.document.mapping.ColumnValueExtractorException;
import com.exasol.adapter.document.mapping.MappingErrorBehaviour;
import com.exasol.adapter.document.mapping.PropertyToDecimalColumnMapping;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.BigDecimalLiteral;

class DynamodbPropertyToDecimalColumnValueExtractorTest {

    public static final PropertyToDecimalColumnMapping MAPPING = configureExampleMapping(
            PropertyToDecimalColumnMapping.builder())//
                    .lookupFailBehaviour(MappingErrorBehaviour.ABORT)//
                    .decimalPrecision(10)//
                    .decimalScale(2)//
                    .overflowBehaviour(MappingErrorBehaviour.ABORT)//
                    .notNumericBehaviour(MappingErrorBehaviour.ABORT)//
                    .build();
    public static final DynamodbPropertyToDecimalColumnValueExtractor EXTRACTOR = new DynamodbPropertyToDecimalColumnValueExtractor(
            MAPPING);

    @Test
    void testDecimal() {
        final DynamodbMap testData = new DynamodbMap(Map.of("key", AttributeValueQuickCreator.forNumber("1234.45")));
        final BigDecimalLiteral valueExpression = (BigDecimalLiteral) EXTRACTOR.extractColumnValue(testData,
                new StaticDocumentPathIterator());
        assertThat(valueExpression.getValue(), equalTo(BigDecimal.valueOf(1234.45)));
    }

    @Test
    void testNotANumber() {
        final DynamodbMap testData = new DynamodbMap(
                Map.of("key", AttributeValueQuickCreator.forString("not a number")));
        final StaticDocumentPathIterator iterationState = new StaticDocumentPathIterator();
        final ColumnValueExtractorException exception = assertThrows(ColumnValueExtractorException.class,
                () -> EXTRACTOR.extractColumnValue(testData, iterationState));
        assertThat(exception.getMessage(),
                equalTo("The input value was no number. Try using a different mapping or ignore this error by setting notNumericBehaviour = \"null\"."));
    }
}