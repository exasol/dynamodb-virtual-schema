package com.exasol.adapter.document;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

class DynamodbAdapterPropertiesTest {
    private static final String BUCKETFS_PATH = "/bfsdefault/default/mappings/mapping.json";

    @Test
    void testEmptySchema() {
        final AdapterProperties adapterProperties = new AdapterProperties(Collections.emptyMap());
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(false));
    }

    @Test
    void testHasSchemaDefinitionProperty() {

        final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAPPING", BUCKETFS_PATH));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(true));
    }

    @Test
    void testGetSchemaDefinitionProperty() throws AdapterException {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAPPING", BUCKETFS_PATH));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.getMappingDefinition(), equalTo(BUCKETFS_PATH));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 5, 100 })
    void testGetMaxParallelUdfs(final int value) throws AdapterException {
        final AdapterProperties adapterProperties = new AdapterProperties(
                Map.of("MAX_PARALLEL_UDFS", String.valueOf(value)));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.getMaxParallelUdfs(), equalTo(value));
    }

    @Test
    void testGetMaxParallelUdfsNotNumeric() {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAX_PARALLEL_UDFS", "string value"));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        final AdapterException exception = assertThrows(AdapterException.class,
                dynamodbAdapterProperties::getMaxParallelUdfs);
        assertThat(exception.getMessage(),
                equalTo("Invalid value for property MAX_PARALLEL_UDFS: string value. Only integers are allows."));
    }

    @ParameterizedTest
    @ValueSource(ints = { -2, 0 })
    void testGetMaxParallelUdfsTooFew(final int value) {
        final AdapterProperties adapterProperties = new AdapterProperties(
                Map.of("MAX_PARALLEL_UDFS", String.valueOf(value)));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        final AdapterException exception = assertThrows(AdapterException.class,
                dynamodbAdapterProperties::getMaxParallelUdfs);
        assertThat(exception.getMessage(), equalTo("Invalid value for property MAX_PARALLEL_UDFS: " + value
                + ". Value must be >= 1 or -1 for no limitation."));
    }

    @Test
    void testGetMaxParallelUdfsUnlimited() throws AdapterException {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of("MAX_PARALLEL_UDFS", "-1"));
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.getMaxParallelUdfs(), equalTo(Integer.MAX_VALUE));
    }

    @Test
    void testGetMaxParallelUdfsDefault() throws AdapterException {
        final AdapterProperties adapterProperties = new AdapterProperties(Collections.emptyMap());
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        assertThat(dynamodbAdapterProperties.getMaxParallelUdfs(), equalTo(Integer.MAX_VALUE));
    }
}
