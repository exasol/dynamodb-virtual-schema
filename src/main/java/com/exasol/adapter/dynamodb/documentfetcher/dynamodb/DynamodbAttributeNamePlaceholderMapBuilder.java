package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.HashMap;
import java.util.Map;

/**
 * This class builds a map that maps placeholders that are used in DynamoDB expressions to an DynamoDB attribute name.
 * The map can be passed to the DynamoDB API.
 */
public class DynamodbAttributeNamePlaceholderMapBuilder {
    private final Map<String, String> valueMap = new HashMap<>();
    private final Map<String, String> invertedMap = new HashMap<>();
    private int counter = 0;

    /**
     * Adds an attribute name to the placeholder map and gives a placeholder for it.
     * 
     * @param nameToAdd attribute name to be replaced by a placeholder
     * @return placeholder for the attribute name
     */
    public String addValue(final String nameToAdd) {
        if (this.invertedMap.containsKey(nameToAdd)) {
            return this.invertedMap.get(nameToAdd);
        }
        final String key = "#" + this.counter++;
        this.valueMap.put(key, nameToAdd);
        this.invertedMap.put(nameToAdd, key);
        return key;
    }

    /**
     * Gives the built placeholder map.
     *
     * @return placeholder map
     */
    public Map<String, String> getValueMap() {
        return this.valueMap;
    }
}
