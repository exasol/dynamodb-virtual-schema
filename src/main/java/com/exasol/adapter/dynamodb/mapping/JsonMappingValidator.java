package com.exasol.adapter.dynamodb.mapping;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.everit.json.schema.*;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Validator for mapping definitions using a JSON-schema validator.
 */
public class JsonMappingValidator {
	/**
	 * Validates the given schema using a JSON-schema validator.
	 * 
	 * @param schemaMappingDefinition
	 *            schema mapping definition to validate
	 * @throws IOException
	 *             if schema definition could not be opened
	 * @throws JsonMappingFactory.MappingException
	 *             if schema is violated
	 */
	public void validate(final JSONObject schemaMappingDefinition)
			throws IOException, JsonMappingFactory.MappingException {
		final ClassLoader classLoader = JsonMappingFactory.class.getClassLoader();
		try (final InputStream inputStream = classLoader.getResourceAsStream("mappingLanguageSchema.json")) {
			final JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
			final Schema schema = SchemaLoader.load(rawSchema);
			final Validator validator = Validator.builder().build();
			validator.performValidation(schema, schemaMappingDefinition);
		} catch (final ValidationException e) {
			throw new JsonMappingFactory.MappingException(extractReadableErrorMessage(e));
		}
	}

	private String extractReadableErrorMessage(final ValidationException e) {
		final List<ValidationException> causingExceptions = e.getCausingExceptions();
		if (!causingExceptions.isEmpty()) {
			final ValidationException firstException = causingExceptions.get(0);
			return extractReadableErrorMessage(firstException);
		}
		if (e.getErrorMessage().startsWith("extraneous key")
				&& e.getSchemaLocation().equals("#/definitions/mappingDefinition")) {
			final String possibleProperties = possibleObjectProperties(e.getViolatedSchema());
			if (!possibleProperties.isEmpty()) {
				return e.getMessage() + ", use one of the following mapping definitions here: " + possibleProperties;
			}
		}
		if (e.getMessage().startsWith("#/$schema:") && e.getMessage().endsWith("is not a valid enum value")) {
			return e.getPointerToViolation()
					+ " $schema must be set  to https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json";
		}
		if (e.getPointerToViolation().endsWith("/mapping") && e.getKeyword().equals("minProperties")) {
			final String possibleProperties = possibleObjectProperties(e.getViolatedSchema());
			return e.getPointerToViolation() + " please specify at least one mapping here. Possible are: "
					+ possibleProperties;
		}
		return e.getMessage();
	}

	private String possibleObjectProperties(final Schema schema) {
		final Set<String> possibleProperties = new HashSet<>();
		try {
			final ObjectSchema objectSchema = (ObjectSchema) schema;
			possibleProperties.addAll(objectSchema.getPropertySchemas().keySet());
			possibleProperties.addAll(possibleAdditionalObjectProperties(objectSchema));
		} catch (final ClassCastException ignored) {
		}
		return String.join(", ", possibleProperties);
	}

	private Set<String> possibleAdditionalObjectProperties(final ObjectSchema objectSchema) {
		try {
			final ObjectSchema additionalPropertiesSchema = getObjectSchema(
					objectSchema.getSchemaOfAdditionalProperties());
			return additionalPropertiesSchema.getPropertySchemas().keySet();
		} catch (final ClassCastException ignored) {
			return Set.of();
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
