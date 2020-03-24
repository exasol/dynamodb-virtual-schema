package com.exasol.adapter.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonMappingValidatorTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonMappingValidatorTest.class);

	void runValidation(final String fileName) throws IOException, JsonMappingProvider.MappingException {
		final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
		final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
		try (final InputStream inputStream = classLoader.getResourceAsStream(fileName)) {
			jsonMappingValidator.validate(new JSONObject(new JSONTokener(inputStream)));
		} catch (final JsonMappingProvider.MappingException e) {
			LOGGER.info(e.getMessage());
			throw e;
		}
	}

	@Test
	void testValidBasicMapping() throws IOException, JsonMappingProvider.MappingException {
		runValidation("basicMapping.json");
	}

	@Test
	void testValidToJsonMapping() throws IOException, JsonMappingProvider.MappingException {
		runValidation("toJsonMapping.json");
	}

	@Test
	void testValidSingleColumnToTableMapping() throws IOException, JsonMappingProvider.MappingException {
		runValidation("singleColumnToTableMapping.json");
	}

	@Test
	void testValidMultiColumnToTableMapping() throws IOException, JsonMappingProvider.MappingException {
		runValidation("multiColumnToTableMapping.json");
	}

	@Test
	void testValidWholeTableToJsonMapping() throws IOException, JsonMappingProvider.MappingException {
		runValidation("wholeTableTojsonMapping.json");
	}

	@Test
	void testInvalidNoDestName() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingNoDestTableName.json"));
		assertThat(exception.getMessage(), equalTo("#: required key [destTableName] not found"));
	}

	@Test
	void testInvalidNoSchemaSet() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingNoSchemaSet.json"));
		assertThat(exception.getMessage(), equalTo("#: required key [$schema] not found"));
	}

	@Test
	void testInvalidWrongSchemaSet() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingWrongSchemaSet.json"));
		assertThat(exception.getMessage(), equalTo(
				"#/$schema $schema must be set  to https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json"));
	}

	@Test
	void testInvalidUnknownRootProperty() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingUnknownRootProperty.json"));
		assertThat(exception.getMessage(), equalTo("#: extraneous key [unknownProperty] is not permitted"));
	}

	@Test
	void testInvalidUnknownMappingType() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingUnknownMappingType.json"));
		assertThat(exception.getMessage(), equalTo(
				"#/mapping/children/isbn: extraneous key [toStriiiiiiingMapping] is not permitted, use one of the following mapping definitions here: toTableMapping, children, toJsonMapping, toStringMapping"));
	}

	@Test
	void testInvalidToTableWithNoChildren() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidToTableMappingWithNoChildren.json"));
		assertThat(exception.getMessage(), equalTo(
				"#/mapping/children/topics/toTableMapping/mapping please specify at least one mapping here. Possible are: toTableMapping, children, toJsonMapping, toStringMapping"));
	}

	@Test
	void testInvalidNoChildren() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingNoMapping.json"));
		assertThat(exception.getMessage(), equalTo("#: required key [mapping] not found"));
	}

	@Test
	void testInvalidUnknownMappingInToTable() throws IOException {
		final JsonMappingProvider.MappingException exception = assertThrows(JsonMappingProvider.MappingException.class,
				() -> runValidation("invalidMappingUnknownMappingTypeInToTable.json"));
		assertThat(exception.getMessage(), equalTo(
				"#/mapping/children/topics/toTableMapping/mapping: extraneous key [toStriiiiingMapping] is not permitted, use one of the following mapping definitions here: toTableMapping, children, toJsonMapping, toStringMapping"));
	}
}
