package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterProperties;

/**
 * This is an adapter for the {@link AdapterProperties} adding some DynamoDB specific properties.
 */
public class DynamodbAdapterProperties {
    private static final String DYNAMODB_SCHEMA = "DYNAMODB_SCHEMA";
    private final AdapterProperties properties;

    /**
     * Constructor
     * @param properties Adapter Properties
     */
    public DynamodbAdapterProperties(final AdapterProperties properties){
        this.properties = properties;
    }

    public boolean hasSchemaDefinition(){
        return this.properties.containsKey(DYNAMODB_SCHEMA);
    }

    public String getSchemaDefinition(){
        return this.properties.get(DYNAMODB_SCHEMA);
    }

}
