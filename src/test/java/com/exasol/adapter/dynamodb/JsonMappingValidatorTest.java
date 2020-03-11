package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.io.InputStream;

import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonMappingValidatorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMappingValidatorTest.class);

    void runValidation(final String fileName) throws IOException, JsonMappingValidator.MappingException {
        final ClassLoader classLoader = DynamodbTestUtilsTestIT.class.getClassLoader();
        final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
        try (final InputStream inputStream = classLoader.getResourceAsStream(fileName)) {
            jsonMappingValidator.validate(new JSONObject(new JSONTokener(inputStream)));
        }
        catch(final JsonMappingValidator.MappingException e){
            LOGGER.info(e.getMessage());
            throw e;
        }
    }

    @Test
    void testValidBasicMapping() throws IOException, JsonMappingValidator.MappingException {
        runValidation("basicMapping.json");
    }

    @Test
    void testValidToJsonMapping() throws IOException, JsonMappingValidator.MappingException {
        runValidation("toJsonMapping.json");
    }

    @Test
    void testValidToTableMapping() throws IOException, JsonMappingValidator.MappingException {
        runValidation("toTableMapping.json");
    }

    @Test
    void testInvalidNoDestName() throws IOException {
        final JsonMappingValidator.MappingException exception = assertThrows(JsonMappingValidator.MappingException.class, () -> runValidation("invalidMappingNoDestTableName.json"));
        assertThat(exception.getMessage(), equalTo("required key [destTableName] not found"));
    }

    @Test
    void testInvalidNoSchemaSet() throws IOException {
        final JsonMappingValidator.MappingException exception = assertThrows(JsonMappingValidator.MappingException.class, () -> runValidation("invalidMappingNoSchemaSet.json"));
        assertThat(exception.getMessage(), equalTo("required key [$schema] not found"));
    }

    @Test
    void testInvalidWrongSchemaSet() throws IOException {
        final JsonMappingValidator.MappingException exception = assertThrows(JsonMappingValidator.MappingException.class, () -> runValidation("invalidMappingWrongSchemaSet.json"));
        assertThat(exception.getMessage(), equalTo("#/$schema: https://github.com/unsupported/schema is not a valid enum value"));
    }

    @Test
    void testInvalidUnknownRootProperty() throws IOException {
        final JsonMappingValidator.MappingException exception = assertThrows(JsonMappingValidator.MappingException.class, () -> runValidation("invalidMappingUnknownRootProperty.json"));
        assertThat(exception.getMessage(), equalTo("#: extraneous key [unknownProperty] is not permitted"));
    }

    @Test
    void testInvalidUnknownMappingType() throws IOException {
        final JsonMappingValidator.MappingException exception = assertThrows(JsonMappingValidator.MappingException.class, () -> runValidation("invalidMappingUnknownMappingType.json"));
        assertThat(exception.getMessage(), equalTo("#/children/isbn unknown mapping type"));
    }

    @Test
    void testInvalidToTableWithNoChildren() throws IOException {
        final JsonMappingValidator.MappingException exception = assertThrows(JsonMappingValidator.MappingException.class, () -> runValidation("invalidToTableMappingWithNoChildren.json"));
        assertThat(exception.getMessage(), equalTo("#/children/isbn unknown mapping type"));
    }
}
