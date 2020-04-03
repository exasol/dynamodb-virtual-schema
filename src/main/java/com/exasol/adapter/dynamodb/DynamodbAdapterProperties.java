package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

/**
 * This is an adapter for the {@link AdapterProperties} adding some DynamoDB
 * specific properties.
 */
public class DynamodbAdapterProperties {
	private static final String MAPPING_KEY = "MAPPING";
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
	 * Check if the mapping definition property is set.
	 *
	 * @return <code>true</code> if schema definition property is set
	 */
	public boolean hasMappingDefinition() {
		return this.properties.containsKey(MAPPING_KEY);
	}

	/**
	 * Get mapping definition property.
	 *
	 * @return String path to mapping definition files on bucketfs
	 */
	public String getMappingDefinition() throws AdapterException {
		if (!this.hasMappingDefinition()) {
			throw new AdapterException(
					MAPPING_KEY + " is mandatory. Provide the path to your schema mapping files on bucketfs here.");
		}
		final String property = this.properties.get(MAPPING_KEY);
		if (property.isEmpty()) {
			throw new AdapterException(MAPPING_KEY
					+ " must not be empty. Provide the path to your schema mapping files on bucketfs here.");
		}
		return property;
	}
}
