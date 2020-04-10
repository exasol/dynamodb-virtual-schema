package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonMappingValidatorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMappingValidatorTest.class);
    private final MappingTestFiles mappingTestFiles = new MappingTestFiles();

    void runValidation(final File file) throws IOException {
        final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
        try {
            jsonMappingValidator.validate(file);
        } catch (final IllegalArgumentException exception) {
            LOGGER.info(exception.getMessage());
            throw exception;
        }
    }

    @AfterEach
    void afterEach() {
        this.mappingTestFiles.deleteAllTempFiles();
    }

    @Test
    void testValidBasicMapping() throws IOException {
        runValidation(MappingTestFiles.BASIC_MAPPING_FILE);
    }

    @Test
    void testValidToJsonMapping() throws IOException {
        runValidation(MappingTestFiles.TO_JSON_MAPPING_FILE);
    }

    @Test
    void testValidSingleColumnToTableMapping() throws IOException {
        runValidation(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE);
    }

    @Test
    void testValidMultiColumnToTableMapping() throws IOException {
        runValidation(MappingTestFiles.MULTI_COLUMN_TO_TABLE_MAPPING_FILE);
    }

    @Test
    void testValidWholeTableToJsonMapping() throws IOException {
        runValidation(MappingTestFiles.WHOLE_TABLE_TO_TABLE_MAPPING_FILE);
    }

    private void testInvalid(final File base, final Function<JSONObject, JSONObject> invalidator,
            final String expectedMessage) throws IOException {
        final File invalidFile = this.mappingTestFiles.generateInvalidFile(base, invalidator);
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> runValidation(invalidFile));
        assertThat(exception.getMessage(),
                equalTo("Syntax error in " + invalidFile.getName() + ": " + expectedMessage));
    }

    @Test
    void testInvalidNoDestName() throws IOException {
        testInvalid(MappingTestFiles.BASIC_MAPPING_FILE, base -> {
            base.remove("destTable");
            return base;
        }, "#: required key [destTable] not found");
    }

    @Test
    void testInvalidNoSchemaSet() throws IOException {
        testInvalid(MappingTestFiles.BASIC_MAPPING_FILE, base -> {
            base.remove("$schema");
            return base;
        }, "#: required key [$schema] not found");
    }

    @Test
    void testInvalidWrongSchemaSet() throws IOException {
        testInvalid(MappingTestFiles.BASIC_MAPPING_FILE, base -> {
            base.put("$schema", "wrongSchema");
            return base;
        }, "#/$schema $schema must be set  to https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json");
    }

    @Test
    void testInvalidUnknownRootProperty() throws IOException {
        testInvalid(MappingTestFiles.BASIC_MAPPING_FILE, base -> {
            base.put("unknownProperty", "someValue");
            return base;
        }, "#: extraneous key [unknownProperty] is not permitted");
    }

    @Test
    void testInvalidUnknownMappingType() throws IOException {
        testInvalid(MappingTestFiles.BASIC_MAPPING_FILE, base -> {
            final JSONObject isbn = base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("isbn");
            isbn.remove("toStringMapping");
            isbn.put("toStriiiiiiingMapping", "");
            return base;
        }, "#/mapping/fields/isbn: extraneous key [toStriiiiiiingMapping] is not permitted, use one of the following mapping definitions: toTableMapping, toJsonMapping, fields, toStringMapping");
    }

    @Test
    void testInvalidToTableWithNoFields() throws IOException {
        testInvalid(MappingTestFiles.MULTI_COLUMN_TO_TABLE_MAPPING_FILE, base -> {
            base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("chapters")
                    .getJSONObject("toTableMapping").getJSONObject("mapping").remove("fields");
            return base;
        }, "#/mapping/fields/chapters/toTableMapping/mapping Please specify at least one mapping. Possible are: toTableMapping, toJsonMapping, fields, toStringMapping");
    }

    @Test
    void testInvalidNoMapping() throws IOException {
        testInvalid(MappingTestFiles.BASIC_MAPPING_FILE, base -> {
            base.remove("mapping");
            return base;
        }, "#: required key [mapping] not found");
    }

    @Test
    void testInvalidUnknownMappingInToTable() throws IOException {
        testInvalid(MappingTestFiles.MULTI_COLUMN_TO_TABLE_MAPPING_FILE, base -> {
            final JSONObject mapping = base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("chapters")
                    .getJSONObject("toTableMapping").getJSONObject("mapping");
            mapping.remove("fields");
            mapping.put("toStriiiiingMapping", "");
            return base;
        }, "#/mapping/fields/chapters/toTableMapping/mapping: extraneous key [toStriiiiingMapping] is not permitted, use one of the following mapping definitions: toTableMapping, toJsonMapping, fields, toStringMapping");
    }
}
