package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.dynamodb.DynamodbToJsonValueMapper;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.dynamodb.DynamodbToStringValueMapper;

public class DynamodbValueMapperFactoryTest {

    private static final AbstractColumnMappingDefinition.ConstructorParameters COLUMN_PARAMETERS = new AbstractColumnMappingDefinition.ConstructorParameters(
            "", null, null);

    @Test
    void testToStringMapping() {
        final ToStringColumnMappingDefinition mappingDefinition = new ToStringColumnMappingDefinition(COLUMN_PARAMETERS,
                10, null);
        final AbstractValueMapper valueMapper = new DynamodbValueMapperFactory()
                .getValueMapperForColumn(mappingDefinition);
        assertThat(valueMapper.getClass(), equalTo(DynamodbToStringValueMapper.class));
    }

    @Test
    void testToJsonMapping() {
        final ToJsonColumnMappingDefinition mappingDefinition = new ToJsonColumnMappingDefinition(COLUMN_PARAMETERS);
        final AbstractValueMapper valueMapper = new DynamodbValueMapperFactory()
                .getValueMapperForColumn(mappingDefinition);
        assertThat(valueMapper.getClass(), equalTo(DynamodbToJsonValueMapper.class));
    }
}
