package com.exasol.adapter.dynamodb.mapping.dynamodb;

import static com.exasol.adapter.dynamodb.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentpath.StaticDocumentPathIterator;
import com.exasol.adapter.dynamodb.mapping.ColumnValueExtractorException;
import com.exasol.adapter.dynamodb.mapping.MappingErrorBehaviour;
import com.exasol.adapter.dynamodb.mapping.ToDecimalPropertyToColumnMapping;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.BigDecimalLiteral;

class DynamodbToDecimalPropertyToColumnValueExtractorTest {

    public static final ToDecimalPropertyToColumnMapping MAPPING = configureExampleMapping(
            ToDecimalPropertyToColumnMapping.builder())//
                    .lookupFailBehaviour(MappingErrorBehaviour.ABORT)//
                    .decimalPrecision(10)//
                    .decimalScale(2)//
                    .overflowBehaviour(MappingErrorBehaviour.ABORT)//
                    .notANumberBehaviour(MappingErrorBehaviour.ABORT)//
                    .build();
    public static final DynamodbToDecimalPropertyToColumnValueExtractor EXTRACTOR = new DynamodbToDecimalPropertyToColumnValueExtractor(
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
                equalTo("Only DynamoDB numbers can be converted to DECIMAL. Try using a different mapping."));
    }
}