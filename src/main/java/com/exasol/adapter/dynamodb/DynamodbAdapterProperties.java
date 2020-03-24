package com.exasol.adapter.dynamodb;

import java.io.File;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

/**
 * This is an adapter for the {@link AdapterProperties} adding some DynamoDB
 * specific properties.
 */
public class DynamodbAdapterProperties {
	private static final String BUCKETFS_BASIC_PATH = "/exa/data/bucketfs/bfsdefault";
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
	public File getMappingDefinition() throws AdapterException {
		if (!this.hasMappingDefinition()) {
			throw new AdapterException(String.format(
					"%s is mandatory. Provide the path to your schema mapping files on bucketfs here.", MAPPING_KEY));
		}
		final String property = this.properties.get(MAPPING_KEY);
		if (property.isEmpty()) {
			throw new AdapterException(String.format(
					"%s must not be empty. Provide the path to your schema mapping files on bucketfs here.",
					MAPPING_KEY));
		}
		return null;
	}

}
