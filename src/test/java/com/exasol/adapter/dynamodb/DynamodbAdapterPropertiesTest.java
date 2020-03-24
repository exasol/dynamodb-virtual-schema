package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

/**
 * Tests for {@link DynamodbAdapterProperties}.
 */
public class DynamodbAdapterPropertiesTest {
	@Test
	public void testEmptySchema() {
		final AdapterProperties adapterProperties = new AdapterProperties(Collections.emptyMap());
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(false));
	}

	@Test
	public void testHasSchemaDefinitionProperty() {
		final String value = "(myString VARCHAR(255))";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("DYNAMODB_SCHEMA", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.hasMappingDefinition(), equalTo(true));
	}

	@Test
	public void testGetSchemaDefinitionProperty() {
		final String value = "(myString VARCHAR(255))";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("DYNAMODB_SCHEMA", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.getMappingDefinition(), equalTo(value));
	}
}
