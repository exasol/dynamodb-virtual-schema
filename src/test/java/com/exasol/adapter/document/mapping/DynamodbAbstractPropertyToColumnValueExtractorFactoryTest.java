package com.exasol.adapter.document.mapping;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbPropertyToJsonColumnValueExtractor;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbPropertyToVarcharColumnValueExtractor;

class DynamodbAbstractPropertyToColumnValueExtractorFactoryTest {

    @Test
    void testToStringMapping() {
        final PropertyToVarcharColumnMapping mappingDefinition = configureExampleMapping(
                PropertyToVarcharColumnMapping.builder()).build();
        final ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor = new DynamodbPropertyToColumnValueExtractorFactory()
                .getValueExtractorForColumn(mappingDefinition);
        assertThat(columnValueExtractor.getClass(), equalTo(DynamodbPropertyToVarcharColumnValueExtractor.class));
    }

    @Test
    void testToJsonMapping() {
        final PropertyToJsonColumnMapping mappingDefinition = configureExampleMapping(
                PropertyToJsonColumnMapping.builder()).build();
        final ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor = new DynamodbPropertyToColumnValueExtractorFactory()
                .getValueExtractorForColumn(mappingDefinition);
        assertThat(columnValueExtractor.getClass(), equalTo(DynamodbPropertyToJsonColumnValueExtractor.class));
    }
}
