package com.exasol.adapter.dynamodb.mapping.reader;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.*;

/**
 * This class creates {@link ColumnMapping}s from a JSON definition. It is used in the {@link JsonSchemaMappingReader}.
 */
class ColumnMappingReader {
    private static final ColumnMappingReader INSTANCE = new ColumnMappingReader();
    private static final String MAX_LENGTH_KEY = "maxLength";
    private static final int DEFAULT_MAX_LENGTH = 254;
    private static final String OVERFLOW_KEY = "overflow";
    private static final String OVERFLOW_ABORT = "ABORT";
    private static final String DEST_NAME_KEY = "destName";
    private static final String REQUIRED_KEY = "required";
    private static final String TO_STRING_MAPPING_KEY = "toStringMapping";
    private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";
    private static final TruncateableMappingErrorBehaviour DEFAULT_TO_STRING_OVERFLOW = TruncateableMappingErrorBehaviour.TRUNCATE;
    private static final MappingErrorBehaviour DEFAULT_LOOKUP_BEHAVIOUR = MappingErrorBehaviour.NULL;

    /**
     * Private constructor to hide the public default. Get an instance using {@link #getInstance()}.
     */
    private ColumnMappingReader() {
        // empty on purpose
    }

    /**
     * Get a singleton instance of {@link ColumnMappingReader}.
     * 
     * @return singleton instance of {@link ColumnMappingReader}
     */
    public static ColumnMappingReader getInstance() {
        return INSTANCE;
    }

    ColumnMapping readColumnMapping(final String mappingKey, final JsonObject definition,
            final DocumentPathExpression.Builder sourcePath, final String propertyName, final boolean isRootLevel) {
        switch (mappingKey) {
        case TO_STRING_MAPPING_KEY:
            return readStringColumnIfPossible(definition, sourcePath, propertyName, isRootLevel);
        case TO_JSON_MAPPING_KEY:
            return readToJsonColumn(definition, sourcePath, propertyName);
        default:
            throw new UnsupportedOperationException(
                    "This mapping type (" + mappingKey + ") is not supported in the current version.");
        }
    }

    private ToStringPropertyToColumnMapping readStringColumnIfPossible(final JsonObject definition,
            final DocumentPathExpression.Builder sourcePath, final String dynamodbPropertyName,
            final boolean isRootLevel) {
        if (isRootLevel) {
            throw new ExasolDocumentMappingLanguageException(
                    "ToStringMapping is not allowed at root level. You probably want to replace it with a \"fields\" definition.");
        }
        return addStringColumn(definition, sourcePath, dynamodbPropertyName);
    }

    private ToStringPropertyToColumnMapping addStringColumn(final JsonObject definition,
            final DocumentPathExpression.Builder sourcePath, final String dynamodbPropertyName) {
        final int maxLength = definition.getInt(MAX_LENGTH_KEY, DEFAULT_MAX_LENGTH);
        final TruncateableMappingErrorBehaviour overflowBehaviour = readStringOverflowBehaviour(
                definition);
        final String exasolColumnName = readExasolColumnName(definition, dynamodbPropertyName);
        final MappingErrorBehaviour lookupFailBehaviour = readMappingErrorBehaviour(definition);
        return new ToStringPropertyToColumnMapping(exasolColumnName, sourcePath.build(), lookupFailBehaviour, maxLength,
                overflowBehaviour);
    }

    private TruncateableMappingErrorBehaviour readStringOverflowBehaviour(final JsonObject definition) {
        if (definition.containsKey(OVERFLOW_KEY) && definition.getString(OVERFLOW_KEY).equals(OVERFLOW_ABORT)) {
            return TruncateableMappingErrorBehaviour.ABORT;
        } else {
            return DEFAULT_TO_STRING_OVERFLOW;
        }
    }

    private MappingErrorBehaviour readMappingErrorBehaviour(final JsonObject definition) {
        if (definition.containsKey(REQUIRED_KEY) && definition.getBoolean(REQUIRED_KEY)) {
            return MappingErrorBehaviour.ABORT;
        } else {
            return DEFAULT_LOOKUP_BEHAVIOUR;
        }
    }

    private String readExasolColumnName(final JsonObject definition, final String defaultValue) {
        final String exasolColumnName = definition.getString(DEST_NAME_KEY, defaultValue);
        if (exasolColumnName == null) {
            throw new ExasolDocumentMappingLanguageException(DEST_NAME_KEY
                    + " is mandatory in this definition. Please set it to the desired name for the Exasol column.");
        }
        return exasolColumnName.toUpperCase();
    }

    private ToJsonPropertyToColumnMapping readToJsonColumn(final JsonObject definition,
            final DocumentPathExpression.Builder sourcePath, final String dynamodbPropertyName) {
        final String exasolColumnName = readExasolColumnName(definition, dynamodbPropertyName);
        final MappingErrorBehaviour lookupFailBehaviour = readMappingErrorBehaviour(definition);
        final MappingErrorBehaviour overflowBehaviour = definition.getString(OVERFLOW_KEY, "")
                .equalsIgnoreCase(OVERFLOW_ABORT) ? MappingErrorBehaviour.ABORT : MappingErrorBehaviour.NULL;
        return new ToJsonPropertyToColumnMapping(exasolColumnName, sourcePath.build(), lookupFailBehaviour,
                definition.getInt(MAX_LENGTH_KEY, DEFAULT_MAX_LENGTH), overflowBehaviour);
    }
}
