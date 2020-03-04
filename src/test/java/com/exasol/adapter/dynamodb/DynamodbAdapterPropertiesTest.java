package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

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
	public void testSchemaProperty() {
		final String value = "(myString VARCHAR(255))";
		final AdapterProperties adapterProperties = new AdapterProperties(Map.of("DYNAMODB_SCHEMA", value));
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		assertAll(() -> assertThat(dynamodbAdapterProperties.hasSchemaDefinition(), equalTo(true)),
				() -> assertThat(dynamodbAdapterProperties.getSchemaDefinition(), equalTo(value)));
	}

}
