package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

/**
 * Tests for {@link DynamodbAdapterProperties}.
 */
public class DynamodbAdapterPropertiesTest {
	@Test
	public void testEmptySchema() {
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of());
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.hasSchemaDefinition(), equalTo(false));
	}

	@Test
	public void testHasSchemaDefinitionProperty() {
		final String value = "(myString VARCHAR(255))";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("DYNAMODB_SCHEMA", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.hasSchemaDefinition(), equalTo(true));
	}

	@Test
	public void testGetSchemaDefinitionProperty() {
		final String value = "(myString VARCHAR(255))";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("DYNAMODB_SCHEMA", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertThat(dynamodbAdapterProperties.getSchemaDefinition(), equalTo(value));
	}
}
