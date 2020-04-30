package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

public class DynamodbAdapterPropertiesTest {
    private static final String BUCKETFS_PATH = "/bfsdefault/default/mappings/mapping.json";

    @Test
    public void testEmptySchema() {
        final AdapterProperties adapterProperties = new AdapterProperties(Collections.emptyMap());
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(false));
    }

    @Test
    public void testHasSchemaDefinitionProperty() {

        final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAPPING", BUCKETFS_PATH));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(true));
    }

    @Test
    public void testGetSchemaDefinitionProperty() throws AdapterException {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAPPING", BUCKETFS_PATH));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.getMappingDefinition(), equalTo(BUCKETFS_PATH));
    }
}
