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

    void runValidation(final String fileName) throws IOException {
        final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
        final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
        final File file = new File(classLoader.getResource(fileName).getFile());
        try {
            jsonMappingValidator.validate(file);
        } catch (final IllegalArgumentException exception) {
            LOGGER.info(exception.getMessage());
            throw exception;
        }
    }

    @Test
    void testValidBasicMapping() throws IOException {
        runValidation("basicMapping.json");
    }

    @Test
    void testValidToJsonMapping() throws IOException {
        runValidation("toJsonMapping.json");
    }

    @Test
    void testValidSingleColumnToTableMapping() throws IOException {
        runValidation("singleColumnToTableMapping.json");
    }

    @Test
    void testValidMultiColumnToTableMapping() throws IOException {
        runValidation("multiColumnToTableMapping.json");
    }

    @Test
    void testValidWholeTableToJsonMapping() throws IOException {
        runValidation("wholeTableToJsonMapping.json");
    }

    @Test
    void testInvalidNoDestName() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingNoDestTable.json"));
        assertThat(exception.getMessage(), equalTo("Syntax error in invalidMappingNoDestTable.json: #: required key [destTable] not found"));
    }

    @Test
    void testInvalidNoSchemaSet() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingNoSchemaSet.json"));
        assertThat(exception.getMessage(), equalTo("Syntax error in invalidMappingNoSchemaSet.json: #: required key [$schema] not found"));
    }

    @Test
    void testInvalidWrongSchemaSet() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingWrongSchemaSet.json"));
        assertThat(exception.getMessage(), equalTo(
                "Syntax error in invalidMappingWrongSchemaSet.json: #/$schema $schema must be set  to https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json"));
    }

    @Test
    void testInvalidUnknownRootProperty() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingUnknownRootProperty.json"));
        assertThat(exception.getMessage(), equalTo("Syntax error in invalidMappingUnknownRootProperty.json: #: extraneous key [unknownProperty] is not permitted"));
    }

    @Test
    void testInvalidUnknownMappingType() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingUnknownMappingType.json"));
        assertThat(exception.getMessage(), equalTo(
                "Syntax error in invalidMappingUnknownMappingType.json: #/mapping/fields/isbn: extraneous key [toStriiiiiiingMapping] is not permitted, use one of the following mapping definitions: toTableMapping, toJsonMapping, fields, toStringMapping"));
    }

    @Test
    void testInvalidToTableWithNoFields() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidToTableMappingWithNoFields.json"));
        assertThat(exception.getMessage(), equalTo(
                "Syntax error in invalidToTableMappingWithNoFields.json: #/mapping/fields/topics/toTableMapping/mapping Please specify at least one mapping. Possible are: toTableMapping, toJsonMapping, fields, toStringMapping"));
    }

    @Test
    void testInvalidNoMapping() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingNoMapping.json"));
        assertThat(exception.getMessage(), equalTo("Syntax error in invalidMappingNoMapping.json: #: required key [mapping] not found"));
    }

    @Test
    void testInvalidUnknownMappingInToTable() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation("invalidMappingUnknownMappingTypeInToTable.json"));
        assertThat(exception.getMessage(), equalTo(
                "Syntax error in invalidMappingUnknownMappingTypeInToTable.json: #/mapping/fields/topics/toTableMapping/mapping: extraneous key [toStriiiiingMapping] is not permitted, use one of the following mapping definitions: toTableMapping, toJsonMapping, fields, toStringMapping"));
    }
}
