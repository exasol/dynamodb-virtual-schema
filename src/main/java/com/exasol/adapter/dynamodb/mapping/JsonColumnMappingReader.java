package com.exasol.adapter.dynamodb.mapping;

import javax.json.JsonObject;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

/**
 * This class read {@link ColumnMapping}s from a JSON definition. It is used in the {@link JsonSchemaMappingReader}.
 */
class JsonColumnMappingReader {
    private static final String MAX_LENGTH_KEY = "maxLength";
    private static final int DEFAULT_MAX_LENGTH = 254;
    private static final String OVERFLOW_KEY = "overflow";
    private static final String DEST_NAME_KEY = "destName";
    private static final String REQUIRED_KEY = "required";
    private static final ToStringPropertyToColumnMapping.OverflowBehaviour DEFAULT_TO_STRING_OVERFLOW = ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE;
    private static final LookupFailBehaviour DEFAULT_LOOKUP_BEHAVIOUR = LookupFailBehaviour.DEFAULT_VALUE;

    ToStringPropertyToColumnMapping readStringColumnIfPossible(final JsonObject definition,
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
        final ToStringPropertyToColumnMapping.OverflowBehaviour overflowBehaviour = readStringOverflowBehaviour(
                definition);
        final AbstractPropertyToColumnMapping.ConstructorParameters columnParameters = readColumnProperties(definition,
                sourcePath.build(), dynamodbPropertyName);
        return new ToStringPropertyToColumnMapping(columnParameters, maxLength, overflowBehaviour);
    }

    private ToStringPropertyToColumnMapping.OverflowBehaviour readStringOverflowBehaviour(final JsonObject definition) {
        if (definition.containsKey(OVERFLOW_KEY) && definition.getString(OVERFLOW_KEY).equals("ABORT")) {
            return ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION;
        } else {
            return DEFAULT_TO_STRING_OVERFLOW;
        }
    }

    private AbstractPropertyToColumnMapping.ConstructorParameters readColumnProperties(final JsonObject definition,
            final DocumentPathExpression sourcePath, final String dynamodbPropertyName) {
        final String exasolColumnName = readExasolColumnName(definition, dynamodbPropertyName);
        final LookupFailBehaviour lookupFailBehaviour = readLookupFailBehaviour(definition);
        return new AbstractPropertyToColumnMapping.ConstructorParameters(exasolColumnName, sourcePath,
                lookupFailBehaviour);
    }

    private LookupFailBehaviour readLookupFailBehaviour(final JsonObject definition) {
        if (definition.containsKey(REQUIRED_KEY) && definition.getBoolean(REQUIRED_KEY)) {
            return LookupFailBehaviour.EXCEPTION;
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

    ToJsonPropertyToColumnMapping readToJsonColumn(final JsonObject definition,
            final DocumentPathExpression.Builder sourcePath, final String dynamodbPropertyName) {
        final AbstractPropertyToColumnMapping.ConstructorParameters columnParameters = readColumnProperties(definition,
                sourcePath.build(), dynamodbPropertyName);
        return new ToJsonPropertyToColumnMapping(columnParameters);
    }
}
