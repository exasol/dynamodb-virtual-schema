package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterProperties;

/**
 * This is an adapter for the {@link AdapterProperties} adding some DynamoDB
 * specific properties.
 */
public class DynamodbAdapterProperties {
	private static final String DYNAMODB_SCHEMA = "DYNAMODB_SCHEMA";
	private final AdapterProperties properties;

	/**
	 * Constructor
	 * 
	 * @param properties
	 *            Adapter Properties
	 */
	public DynamodbAdapterProperties(final AdapterProperties properties) {
		this.properties = properties;
	}

	/**
	 * Check if the schema definition property is set.
	 *
	 * @return <code>true</code> if schema definition property is set
	 */
	public boolean hasSchemaDefinition() {
		return this.properties.containsKey(DYNAMODB_SCHEMA);
	}

	/**
	 * Get schema definition property.
	 *
	 * @return schema definition
	 */
	public String getSchemaDefinition() {
		return this.properties.get(DYNAMODB_SCHEMA);
	}
}
