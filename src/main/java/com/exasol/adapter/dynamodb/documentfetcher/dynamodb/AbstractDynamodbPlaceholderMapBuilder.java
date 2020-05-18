package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.util.HashMap;
import java.util.Map;

/**
 * This class ist the abstract basis for classes that build a map that maps placeholders to an value.
 *
 * @param <T> Type of the value to be mapped
 */
public abstract class AbstractDynamodbPlaceholderMapBuilder<T> {
    private final Map<String, T> valueMap = new HashMap<>();
    private final Map<T, String> invertedMap = new HashMap<>();
    private int counter = 0;

    /**
     * Gives the start character for placeholders. For example {@code #}.
     * 
     * @return start character for the placeholder
     */
    protected abstract String getPlaceholderCharacter();

    /**
     * Adds a value to the placeholder map and gives a placeholder for it.
     * 
     * @param valueToAdd value to be replaced by a placeholder
     * @return placeholder for the value
     */
    public final String addValue(final T valueToAdd) {
        if (this.invertedMap.containsKey(valueToAdd)) {
            return this.invertedMap.get(valueToAdd);
        }
        final String key = getPlaceholderCharacter() + this.counter++;
        this.valueMap.put(key, valueToAdd);
        this.invertedMap.put(valueToAdd, key);
        return key;
    }

    /**
     * Gives the built value map.
     * 
     * @return value map
     */
    public final Map<String, T> getValueMap() {
        return this.valueMap;
    }
}
