package com.exasol.adapter.dynamodb.mapping;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.everit.json.schema.*;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Validator for mapping definitions using a JSON-schema validator.
 * <p>
 * The validator in this packages requires the use of the {@code io.json} API instead of the project-wide {@code javax}
 * API.
 * </p>
 */
public class JsonMappingValidator {
    private static final String MAPPING_LANGUAGE_SCHEMA = "mappingLanguageSchema.json";
    private final Schema schema;

    /**
     * Creates an instance of {@link JsonMappingValidator}.
     * @throws IOException if reading schema from resources fails.
     */
    public JsonMappingValidator() throws IOException {
        final ClassLoader classLoader = JsonMappingValidator.class.getClassLoader();
        try (final InputStream inputStream = classLoader.getResourceAsStream(MAPPING_LANGUAGE_SCHEMA)) {
            final JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
            this.schema = SchemaLoader.load(rawSchema);
        }
    }

    /**
     * Validates the schema from given file using a JSON-schema validator.
     * 
     * @param schemaMappingDefinition schema mapping definition to validate
     * @throws IOException              if schema definition could not be opened
     * @throws IllegalArgumentException if schema is violated
     */
    public void validate(final File schemaMappingDefinition) throws IOException {
        try (final InputStream inputStream = new FileInputStream(schemaMappingDefinition)) {
            final JSONObject definitionObject = new JSONObject(new JSONTokener(inputStream));
            validate(definitionObject, schemaMappingDefinition.getName());
        }
    }

    private void validate(final JSONObject schemaMappingDefinition, final String fileName){
        try {
            final Validator validator = Validator.builder().build();
            validator.performValidation(this.schema, schemaMappingDefinition);
        } catch (final ValidationException exception) {
            makeValidationExceptionMoreReadable(exception, fileName);
        }
    }

    private void makeValidationExceptionMoreReadable(final ValidationException exception, final String fileName) {
        final List<ValidationException> causingExceptions = exception.getCausingExceptions();
        if (!causingExceptions.isEmpty()) {
            final ValidationException firstException = causingExceptions.get(0);
            makeValidationExceptionMoreReadable(firstException, fileName);
        }
        makeUnknownMappingTypeExceptionMoreReadable(exception, fileName);
        makeWrongSchemaExceptionMoreReadable(exception, fileName);
        makeNoMappingExceptionMoreReadable(exception, fileName);
        throwExceptionWithMappingName(fileName, exception.getMessage());
    }

    private void makeUnknownMappingTypeExceptionMoreReadable(final ValidationException exception,
            final String fileName) {
        if (exception.getErrorMessage().startsWith("extraneous key")
                && exception.getSchemaLocation().equals("#/definitions/mappingDefinition")) {
            final String possibleProperties = possibleObjectProperties(exception.getViolatedSchema());
            if (!possibleProperties.isEmpty()) {
                throwExceptionWithMappingName(fileName, exception.getMessage()
                        + ", use one of the following mapping definitions: " + possibleProperties);
            }
        }
    }

    private void makeWrongSchemaExceptionMoreReadable(final ValidationException exception, final String fileName) {
        if (exception.getMessage().startsWith("#/$schema:")
                && exception.getMessage().endsWith("is not a valid enum value")) {
            throwExceptionWithMappingName(fileName, exception.getPointerToViolation()
                    + " $schema must be set  to https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json");
        }
    }

    private void makeNoMappingExceptionMoreReadable(final ValidationException exception, final String fileName) {
        if (exception.getPointerToViolation().endsWith("/mapping") && exception.getKeyword().equals("minProperties")) {
            final String possibleProperties = possibleObjectProperties(exception.getViolatedSchema());
            throwExceptionWithMappingName(fileName, exception.getPointerToViolation()
                    + " Please specify at least one mapping. Possible are: " + possibleProperties);
        }
    }

    private void throwExceptionWithMappingName(final String fileName, final String message) {
        throw new IllegalArgumentException("Syntax error in " + fileName + ": " + message);
    }

    private String possibleObjectProperties(final Schema schema) {
        try {
            final Set<String> possibleProperties = new HashSet<>();
            final ObjectSchema objectSchema = (ObjectSchema) schema;
            possibleProperties.addAll(objectSchema.getPropertySchemas().keySet());
            possibleProperties.addAll(possibleAdditionalObjectProperties(objectSchema));
            return String.join(", ", possibleProperties);
        } catch (final ClassCastException ignored) {
            return "";
        }
    }

    private Set<String> possibleAdditionalObjectProperties(final ObjectSchema objectSchema) {
        try {
            final ObjectSchema additionalPropertiesSchema = getObjectSchema(
                    objectSchema.getSchemaOfAdditionalProperties());
            return additionalPropertiesSchema.getPropertySchemas().keySet();
        } catch (final ClassCastException ignored) {
            return Collections.emptySet();
        }
    }

    private ObjectSchema getObjectSchema(final Schema schema) {
        if (schema instanceof ObjectSchema) {
            return (ObjectSchema) schema;
        }
        final ReferenceSchema referenceSchema = (ReferenceSchema) schema;
        return (ObjectSchema) referenceSchema.getReferredSchema();
    }
}
