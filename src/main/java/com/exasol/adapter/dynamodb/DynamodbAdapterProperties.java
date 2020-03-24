package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.IOException;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

/**
 * This is an adapter for the {@link AdapterProperties} adding some DynamoDB
 * specific properties.
 */
public class DynamodbAdapterProperties {
	private static final String BUCKETFS_BASIC_PATH = "/buckets";
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
		final String bucketfsPath = BUCKETFS_BASIC_PATH + property;
		final File selectedFile = new File(bucketfsPath);
		preventInjection(selectedFile);
		return selectedFile;
	}

	private void preventInjection(final File file) throws AdapterException {
		try {
			final String absolute;
			absolute = file.getCanonicalPath();
			if (!absolute.startsWith(BUCKETFS_BASIC_PATH)) {
				throw new AdapterException("given path is outside of bucketfs");
			}
		} catch (final IOException e) {
			throw new AdapterException(String.format("error in file path: %s", file.getAbsolutePath()), e);
		}
	}
}
