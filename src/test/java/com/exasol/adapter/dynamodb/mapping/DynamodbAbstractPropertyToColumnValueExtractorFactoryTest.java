package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToJsonColumnValueExtractor;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToVarcharColumnValueExtractor;

class DynamodbAbstractPropertyToColumnValueExtractorFactoryTest {

    @Test
    void testToStringMapping() {
        final PropertyToVarcharColumnMapping mappingDefinition = new PropertyToVarcharColumnMapping("", null, null,
                10, null);
        final ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor = new DynamodbPropertyToColumnValueExtractorFactory()
                .getValueExtractorForColumn(mappingDefinition);
        assertThat(columnValueExtractor.getClass(), equalTo(DynamodbPropertyToVarcharColumnValueExtractor.class));
    }

    @Test
    void testToJsonMapping() {
        final PropertyToJsonColumnMapping mappingDefinition = new PropertyToJsonColumnMapping("", null, null, 0,
                null);
        final ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor = new DynamodbPropertyToColumnValueExtractorFactory()
                .getValueExtractorForColumn(mappingDefinition);
        assertThat(columnValueExtractor.getClass(), equalTo(DynamodbPropertyToJsonColumnValueExtractor.class));
    }
}
