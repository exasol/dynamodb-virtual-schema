package com.exasol.adapter.dynamodb.mapping.reader;

import static com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping.OverflowBehaviour.NULL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.LookupFailBehaviour;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ToStringPropertyToColumnMapping;

class ColumnMappingReaderTest {

    /**
     * Tests for the {@link ColumnMappingReader} uses the correct default values for the
     * {@link ToStringPropertyToColumnMapping}.
     */
    @Test
    void testToStringColumnDefaultValues() {
        assertToStringDefinitionDefaultValues(Json.createObjectBuilder().build());
    }

    @Test
    void testToStringColumnExplicitDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("maxLength", 254)//
                .add("overflow", "TRUNCATE")//
                .add("destName", "test")//
                .add("required", false).build();
        assertToStringDefinitionDefaultValues(definition);
    }

    void assertToStringDefinitionDefaultValues(final JsonObject definition) {
        final ToStringPropertyToColumnMapping columnMapping = (ToStringPropertyToColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toStringMapping", definition, new DocumentPathExpression.Builder(), "test", false);
        assertAll(// )
                () -> assertThat(columnMapping.getMaxSize(), equalTo(254)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.DEFAULT_VALUE)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(254) UTF8"))//
        );
    }

    @Test
    void testToStringColumnExplicitNonDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("maxLength", 123)//
                .add("overflow", "ABORT")//
                .add("destName", "my_column")//
                .add("required", true).build();
        final ToStringPropertyToColumnMapping columnMapping = (ToStringPropertyToColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toStringMapping", definition, new DocumentPathExpression.Builder(), "test", false);
        assertAll(// )
                () -> assertThat(columnMapping.getMaxSize(), equalTo(123)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("MY_COLUMN")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.EXCEPTION)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(123) UTF8"))//
        );
    }

    @Test
    void testUnsupportedMappingType() {
        final DocumentPathExpression.Builder sourcePath = new DocumentPathExpression.Builder();
        final JsonObject definition = Json.createObjectBuilder().build();
        final ColumnMappingReader columnMappingReader = ColumnMappingReader.getInstance();
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> columnMappingReader.readColumnMapping("unknownType", definition, sourcePath, "test", false));
        assertThat(exception.getMessage(),
                equalTo("This mapping type (unknownType) is not supported in the current version."));
    }

    @Test
    void testToJsonColumnDefaultValues() {
        assertToJsonDefinitionDefaultValues(Json.createObjectBuilder().build());
    }

    @Test
    void testToJsonColumnExplicitDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("maxLength", 254)//
                .add("overflow", "TRUNCATE")//
                .add("destName", "test")//
                .add("required", false).build();
        assertToJsonDefinitionDefaultValues(definition);
    }

    void assertToJsonDefinitionDefaultValues(final JsonObject definition) {
        final ToJsonPropertyToColumnMapping columnMapping = (ToJsonPropertyToColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toJsonMapping", definition, new DocumentPathExpression.Builder(), "test", false);
        assertAll(// )
                () -> assertThat(columnMapping.getMaxSize(), equalTo(254)),
                () -> assertThat(columnMapping.getOverflowBehaviour(), equalTo(NULL)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.DEFAULT_VALUE)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(254) UTF8"))//
        );
    }

    @Test
    void testToJsonColumnExplicitNonDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("maxLength", 123)//
                .add("overflow", "ABORT")//
                .add("destName", "my_column")//
                .add("required", true).build();
        final ToJsonPropertyToColumnMapping columnMapping = (ToJsonPropertyToColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toJsonMapping", definition, new DocumentPathExpression.Builder(), "test", false);
        assertAll(// )
                () -> assertThat(columnMapping.getMaxSize(), equalTo(123)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(ToJsonPropertyToColumnMapping.OverflowBehaviour.EXCEPTION)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("MY_COLUMN")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.EXCEPTION)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(123) UTF8"))//
        );
    }
}