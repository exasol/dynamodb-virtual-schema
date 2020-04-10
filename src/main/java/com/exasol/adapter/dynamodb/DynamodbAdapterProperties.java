package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

/**
 * This class adds DynamoDB specific properties to {@link AdapterProperties}.
 */
public class DynamodbAdapterProperties {
    private static final String MAPPING_KEY = "MAPPING";
    private final AdapterProperties properties;

    /**
     * Creates a new instance of {@link DynamodbAdapterProperties}.
     * 
     * @param properties Adapter Properties
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
     * @return String path to mapping definition files in the BucketFS
     */
    public String getMappingDefinition() throws AdapterException {
        if (!hasMappingDefinition()) {
            throw new AdapterException(MAPPING_KEY
                    + " is mandatory. Please set MAPPING to the path to your schema mapping files in the BucketFS.");
        }
        final String property = this.properties.get(MAPPING_KEY);
        if (property.isEmpty()) {
            throw new AdapterException(MAPPING_KEY
                    + " must not be empty. Please set MAPPING to the path to your schema mapping files in the BucketFS.");
        }
        return property;
    }
}
