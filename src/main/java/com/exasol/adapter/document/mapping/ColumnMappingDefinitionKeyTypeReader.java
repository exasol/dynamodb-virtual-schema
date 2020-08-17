package com.exasol.adapter.document.mapping;

import javax.json.JsonObject;

/**
 * This class reads the {@code key} property of an Exasol document mapping language column mapping definition.
 */
public class ColumnMappingDefinitionKeyTypeReader {
    private static final String KEY_KEY = "key";
    private static final String KEY_LOCAL = "local";
    private static final String KEY_GLOBAL = "global";

    /**
     * Reads the {@link KeyType} of a column mapping definition.
     *
     * @param definition the Exasol document mapping language definition of the column
     */
    public KeyType readKeyType(final JsonObject definition) {
        switch (definition.getString(KEY_KEY, "")) {
        case KEY_GLOBAL:
            return KeyType.GLOBAL;
        case KEY_LOCAL:
            return KeyType.LOCAL;
        default:
            return KeyType.NO_KEY;
        }
    }

    /**
     * This enum defines column key types.
     */
    public enum KeyType {
        /**
         * Key type that marks this column as non key column
         */
        NO_KEY,

        /**
         * This key type marks a column a local key column. A local key is unique in the scope of a nested array but not
         * over the whole collection.
         */
        LOCAL,

        /**
         * This key type marks a column as global key column. A global key is unique over all rows. For tables that map
         * nested lists these rows typically result from different documents.
         */
        GLOBAL
    }
}
