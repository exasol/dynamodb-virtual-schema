package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.adapter.dynamodb.DynamodbTestUtilsTestIT;

public class JsonMappingValidatorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMappingValidatorTest.class);

    void runValidation(final String fileName) throws IOException, JsonMappingFactory.MappingException {
        final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
        final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
        final File file = new File(classLoader.getResource(fileName).getFile());
        try {
            jsonMappingValidator.validate(file);
        } catch (final JsonMappingFactory.MappingException exception) {
            LOGGER.info(exception.getMessage());
            throw exception;
        }
    }

    @Test
    void testValidBasicMapping() throws IOException, JsonMappingFactory.MappingException {
        runValidation("basicMapping.json");
    }

    @Test
    void testValidToJsonMapping() throws IOException, JsonMappingFactory.MappingException {
        runValidation("toJsonMapping.json");
    }

    @Test
    void testValidSingleColumnToTableMapping() throws IOException, JsonMappingFactory.MappingException {
        runValidation("singleColumnToTableMapping.json");
    }

    @Test
    void testValidMultiColumnToTableMapping() throws IOException, JsonMappingFactory.MappingException {
        runValidation("multiColumnToTableMapping.json");
    }

    @Test
    void testValidWholeTableToJsonMapping() throws IOException, JsonMappingFactory.MappingException {
        runValidation("wholeTableToJsonMapping.json");
    }

    @Test
    void testInvalidNoDestName() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingNoDestTable.json"));
        assertThat(exception.getMessage(), equalTo("#: required key [destTable] not found"));
    }

    @Test
    void testInvalidNoSchemaSet() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingNoSchemaSet.json"));
        assertThat(exception.getMessage(), equalTo("#: required key [$schema] not found"));
    }

    @Test
    void testInvalidWrongSchemaSet() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingWrongSchemaSet.json"));
        assertThat(exception.getMessage(), equalTo(
                "#/$schema $schema must be set  to https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json"));
    }

    @Test
    void testInvalidUnknownRootProperty() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingUnknownRootProperty.json"));
        assertThat(exception.getMessage(), equalTo("#: extraneous key [unknownProperty] is not permitted"));
    }

    @Test
    void testInvalidUnknownMappingType() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingUnknownMappingType.json"));
        assertThat(exception.getMessage(), equalTo(
                "#/mapping/fields/isbn: extraneous key [toStriiiiiiingMapping] is not permitted, use one of the following mapping definitions: toTableMapping, toJsonMapping, fields, toStringMapping"));
    }

    @Test
    void testInvalidToTableWithNoFields() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidToTableMappingWithNoFields.json"));
        assertThat(exception.getMessage(), equalTo(
                "#/mapping/fields/topics/toTableMapping/mapping Please specify at least one mapping. Possible are: toTableMapping, toJsonMapping, fields, toStringMapping"));
    }

    @Test
    void testInvalidNoMapping() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingNoMapping.json"));
        assertThat(exception.getMessage(), equalTo("#: required key [mapping] not found"));
    }

    @Test
    void testInvalidUnknownMappingInToTable() {
        final JsonMappingFactory.MappingException exception = assertThrows(JsonMappingFactory.MappingException.class,
                () -> runValidation("invalidMappingUnknownMappingTypeInToTable.json"));
        assertThat(exception.getMessage(), equalTo(
                "#/mapping/fields/topics/toTableMapping/mapping: extraneous key [toStriiiiingMapping] is not permitted, use one of the following mapping definitions: toTableMapping, toJsonMapping, fields, toStringMapping"));
    }
}
