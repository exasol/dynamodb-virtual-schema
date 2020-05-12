package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbToJsonValueMapper;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbToStringValueMapper;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbValueMapperFactory;

public class DynamodbAbstractValueMapperFactoryTest {

    @Test
    void testToStringMapping() {
        final ToStringPropertyToColumnMapping mappingDefinition = new ToStringPropertyToColumnMapping("", null, null,
                10, null);
        final ValueExtractor<DynamodbNodeVisitor> valueExtractor = new DynamodbValueMapperFactory()
                .getValueMapperForColumn(mappingDefinition);
        assertThat(valueExtractor.getClass(), equalTo(DynamodbToStringValueMapper.class));
    }

    @Test
    void testToJsonMapping() {
        final ToJsonPropertyToColumnMapping mappingDefinition = new ToJsonPropertyToColumnMapping("", null, null);
        final ValueExtractor<DynamodbNodeVisitor> valueExtractor = new DynamodbValueMapperFactory()
                .getValueMapperForColumn(mappingDefinition);
        assertThat(valueExtractor.getClass(), equalTo(DynamodbToJsonValueMapper.class));
    }
}
