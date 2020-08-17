package com.exasol.adapter.document;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;

/**
 * This class adds DynamoDB specific properties to {@link AdapterProperties}.
 */
public class DynamodbAdapterProperties {
    private static final String MAPPING_KEY = "MAPPING";
    private static final String MAX_PARALLEL_UDFS_KEY = "MAX_PARALLEL_UDFS";
    private final AdapterProperties properties;

    /**
     * Create a new instance of {@link DynamodbAdapterProperties}.
     * 
     * @param properties Adapter Properties
     */
    public DynamodbAdapterProperties(final AdapterProperties properties) {
        this.properties = properties;
    }

    /**
     * Check if the mapping definition property is set.
     *
     * @return {@code true} if schema definition property is set
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

    /**
     * Get MAX_PARALLEL_UDFS property value.
     * 
     * @return configured maximum number of UDFs that are executed in parallel. default: -1
     */
    public int getMaxParallelUdfs() throws AdapterException {
        final String propertyValue = this.properties.get(MAX_PARALLEL_UDFS_KEY);
        final int integerValue = readMaxParallelUdfs(propertyValue);
        if (integerValue == -1) {
            return Integer.MAX_VALUE;
        } else if (integerValue >= 1) {
            return integerValue;
        } else {
            throw new AdapterException("Invalid value for property MAX_PARALLEL_UDFS: " + propertyValue
                    + ". Value must be >= 1 or -1 for no limitation.");
        }
    }

    private int readMaxParallelUdfs(final String propertyValue) throws AdapterException {
        if (propertyValue == null) {
            return Integer.MAX_VALUE;
        } else {
            try {
                return Integer.parseInt(propertyValue);
            } catch (final NumberFormatException exception) {
                throw new AdapterException("Invalid value for property MAX_PARALLEL_UDFS: " + propertyValue
                        + ". Only integers are allows.");
            }
        }
    }
}
