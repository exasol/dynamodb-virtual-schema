package com.exasol.adapter.document.mapping.reader;

import javax.json.JsonObject;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.*;

/**
 * This class creates {@link ColumnMapping}s from a JSON definition. It is used in the {@link JsonSchemaMappingReader}.
 */
class ColumnMappingReader {

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
        return EDML.INSTANCE;
    }

    ColumnMapping readColumnMapping(final String mappingKey, final JsonObject definition,
            final DocumentPathExpression.Builder sourcePath, final String propertyName, final boolean isRootLevel) {
        return readColumnMappingBuilder(mappingKey, definition, isRootLevel)//
                .pathToSourceProperty(sourcePath.build())//
                .exasolColumnName(readExasolColumnName(definition, propertyName))//
                .lookupFailBehaviour(readLookupFailBehaviour(definition)).build();
    }

    private PropertyToColumnMapping.Builder readColumnMappingBuilder(final String mappingKey,
            final JsonObject definition, final boolean isRootLevel) {
        switch (mappingKey) {
        case EDML.TO_VARCHAR_MAPPING_KEY:
            abortIfAtRootLevel(EDML.TO_VARCHAR_MAPPING_KEY, isRootLevel);
            return readToVarcharColumn(definition);
        case EDML.TO_JSON_MAPPING_KEY:
            return readToJsonColumn(definition);
        case EDML.TO_DECIMAL_MAPPING_KEY:
            abortIfAtRootLevel(EDML.TO_DECIMAL_MAPPING_KEY, isRootLevel);
            return readToDecimalColumn(definition);
        default:
            throw new UnsupportedOperationException(
                    "This mapping type (" + mappingKey + ") is not supported in the current version.");
        }
    }

    private PropertyToVarcharColumnMapping.Builder readToVarcharColumn(final JsonObject definition) {
        final PropertyToVarcharColumnMapping.Builder builder = PropertyToVarcharColumnMapping.builder();
        readLookupFailBehaviour(definition);
        return builder.overflowBehaviour(readStringOverflowBehaviour(definition))//
                .varcharColumnSize(readVarcharColumnSize(definition));
    }

    private PropertyToJsonColumnMapping.Builder readToJsonColumn(final JsonObject definition) {
        return PropertyToJsonColumnMapping.builder()//
                .varcharColumnSize(readVarcharColumnSize(definition))//
                .overflowBehaviour(readMappingErrorBehaviour(EDML.OVERFLOW_BEHAVIOUR_KEY, MappingErrorBehaviour.ABORT,
                        definition));
    }

    private PropertyToDecimalColumnMapping.Builder readToDecimalColumn(final JsonObject definition) {
        return PropertyToDecimalColumnMapping.builder()//
                .decimalPrecision(definition.getInt(EDML.DECIMAL_PRECISION_KEY, EDML.DEFAULT_DECIMAL_PRECISION))//
                .decimalScale(definition.getInt(EDML.DECIMAL_SCALE_KEY, EDML.DEFAULT_DECIMAL_SCALE))//
                .overflowBehaviour(
                        readMappingErrorBehaviour(EDML.OVERFLOW_BEHAVIOUR_KEY, MappingErrorBehaviour.ABORT, definition))//
                .notNumericBehaviour(
                        readMappingErrorBehaviour(EDML.NOT_NUMERIC_BEHAVIOUR, MappingErrorBehaviour.ABORT, definition));
    }

    private MappingErrorBehaviour readMappingErrorBehaviour(final String key, final MappingErrorBehaviour defaultValue,
            final JsonObject definition) {
        switch (definition.getString(key, "").toUpperCase()) {
        case EDML.ABORT_KEY:
            return MappingErrorBehaviour.ABORT;
        case EDML.NULL_KEY:
            return MappingErrorBehaviour.NULL;
        default:
            return defaultValue;
        }
    }

    private int readVarcharColumnSize(final JsonObject definition) {
        return definition.getInt(EDML.VARCHAR_COLUMN_SIZE_KEY, EDML.DEFAULT_VARCHAR_COLUMN_SIZE);
    }

    private TruncateableMappingErrorBehaviour readStringOverflowBehaviour(final JsonObject definition) {
        if (definition.containsKey(EDML.OVERFLOW_BEHAVIOUR_KEY)
                && definition.getString(EDML.OVERFLOW_BEHAVIOUR_KEY).equals(EDML.ABORT_KEY)) {
            return TruncateableMappingErrorBehaviour.ABORT;
        } else {
            return EDML.DEFAULT_TO_STRING_OVERFLOW;
        }
    }

    private MappingErrorBehaviour readLookupFailBehaviour(final JsonObject definition) {
        if (definition.containsKey(EDML.REQUIRED_KEY) && definition.getBoolean(EDML.REQUIRED_KEY)) {
            return MappingErrorBehaviour.ABORT;
        } else {
            return EDML.DEFAULT_LOOKUP_BEHAVIOUR;
        }
    }

    private String readExasolColumnName(final JsonObject definition, final String defaultValue) {
        final String exasolColumnName = definition.getString(EDML.DEST_NAME_KEY, defaultValue);
        if (exasolColumnName == null) {
            throw new ExasolDocumentMappingLanguageException(EDML.DEST_NAME_KEY
                    + " is mandatory in this definition. Please set it to the desired name for the Exasol column.");
        }
        return exasolColumnName.toUpperCase();
    }

    private void abortIfAtRootLevel(final String mappingType, final boolean isRootLevel) {
        if (isRootLevel) {
            throw new ExasolDocumentMappingLanguageException(mappingType
                    + " is not allowed at root level. You probably want to replace it with a \"fields\" definition.");
        }
    }

    /**
     * Constants defined by the Exasol Document Mapping Language (EDML) defined in /src/main/resources/v1.json
     */
    private static class EDML {
        private static final ColumnMappingReader INSTANCE = new ColumnMappingReader();
        private static final String VARCHAR_COLUMN_SIZE_KEY = "varcharColumnSize";
        private static final int DEFAULT_VARCHAR_COLUMN_SIZE = 254;
        private static final String OVERFLOW_BEHAVIOUR_KEY = "overflowBehaviour";
        private static final String ABORT_KEY = "ABORT";
        private static final String NULL_KEY = "NULL";
        private static final String DEST_NAME_KEY = "destinationName";
        private static final String REQUIRED_KEY = "required";
        private static final String TO_VARCHAR_MAPPING_KEY = "toVarcharMapping";
        private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";
        private static final String TO_DECIMAL_MAPPING_KEY = "toDecimalMapping";
        private static final TruncateableMappingErrorBehaviour DEFAULT_TO_STRING_OVERFLOW = TruncateableMappingErrorBehaviour.TRUNCATE;
        private static final MappingErrorBehaviour DEFAULT_LOOKUP_BEHAVIOUR = MappingErrorBehaviour.NULL;
        private static final String DECIMAL_PRECISION_KEY = "decimalPrecision";
        private static final String DECIMAL_SCALE_KEY = "decimalScale";
        private static final int DEFAULT_DECIMAL_SCALE = 0;
        private static final int DEFAULT_DECIMAL_PRECISION = 18;
        private static final String NOT_NUMERIC_BEHAVIOUR = "notNumericBehaviour";
    }
}
