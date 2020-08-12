package com.exasol.adapter.dynamodb.mapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.documentpath.StaticDocumentPathIterator;
import com.exasol.adapter.dynamodb.mapping.MappingErrorBehaviour;
import com.exasol.adapter.dynamodb.mapping.PropertyToJsonColumnMapping;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.ValueExpression;

class DynamodbPropertyToJsonColumnValueExtractorTest {
    private static final String DEST_COLUMN = "destColumn";

    @Test
    void testConvertRowBasic() {
        final PropertyToJsonColumnMapping toStringColumnMappingDefinition = new PropertyToJsonColumnMapping(
                DEST_COLUMN, DocumentPathExpression.empty(), MappingErrorBehaviour.ABORT, 100,
                MappingErrorBehaviour.ABORT);
        final DynamodbMap testData = new DynamodbMap(Map.of("key", AttributeValueQuickCreator.forString("value")));
        final ValueExpression exasolCellValue = new DynamodbPropertyToJsonColumnValueExtractor(
                toStringColumnMappingDefinition).extractColumnValue(testData, new StaticDocumentPathIterator());
        assertThat(exasolCellValue.toString(), equalTo("{\"key\":\"value\"}"));
    }
}
