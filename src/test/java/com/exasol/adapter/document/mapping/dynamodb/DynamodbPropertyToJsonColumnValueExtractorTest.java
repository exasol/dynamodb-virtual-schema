package com.exasol.adapter.document.mapping.dynamodb;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.documentpath.StaticDocumentPathIterator;
import com.exasol.adapter.document.mapping.MappingErrorBehaviour;
import com.exasol.adapter.document.mapping.PropertyToJsonColumnMapping;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.ValueExpression;

class DynamodbPropertyToJsonColumnValueExtractorTest {
    private static final String DEST_COLUMN = "destColumn";

    @Test
    void testConvertRowBasic() {
        final PropertyToJsonColumnMapping toStringColumnMappingDefinition = configureExampleMapping(
                PropertyToJsonColumnMapping.builder())//
                        .pathToSourceProperty(DocumentPathExpression.empty()).varcharColumnSize(100)//
                        .overflowBehaviour(MappingErrorBehaviour.ABORT)//
                        .build();
        final FetchedDocument<DynamodbNodeVisitor> testData = new FetchedDocument<>(
                new DynamodbMap(Map.of("key", AttributeValueQuickCreator.forString("value"))), "");
        final ValueExpression exasolCellValue = new DynamodbPropertyToJsonColumnValueExtractor(
                toStringColumnMappingDefinition).extractColumnValue(testData, new StaticDocumentPathIterator());
        assertThat(exasolCellValue.toString(), equalTo("{\"key\":\"value\"}"));
    }
}
