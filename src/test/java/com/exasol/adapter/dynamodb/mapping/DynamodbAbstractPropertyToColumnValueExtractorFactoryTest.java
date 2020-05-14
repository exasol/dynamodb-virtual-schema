package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbToJsonPropertyToColumnValueExtractor;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbToStringPropertyToColumnValueExtractor;

public class DynamodbAbstractPropertyToColumnValueExtractorFactoryTest {

    @Test
    void testToStringMapping() {
        final ToStringPropertyToColumnMapping mappingDefinition = new ToStringPropertyToColumnMapping("", null, null,
                10, null);
        final ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor = new DynamodbPropertyToColumnValueExtractorFactory()
                .getValueExtractorForColumn(mappingDefinition);
        assertThat(columnValueExtractor.getClass(), equalTo(DynamodbToStringPropertyToColumnValueExtractor.class));
    }

    @Test
    void testToJsonMapping() {
        final ToJsonPropertyToColumnMapping mappingDefinition = new ToJsonPropertyToColumnMapping("", null, null);
        final ColumnValueExtractor<DynamodbNodeVisitor> columnValueExtractor = new DynamodbPropertyToColumnValueExtractorFactory()
                .getValueExtractorForColumn(mappingDefinition);
        assertThat(columnValueExtractor.getClass(), equalTo(DynamodbToJsonPropertyToColumnValueExtractor.class));
    }
}
