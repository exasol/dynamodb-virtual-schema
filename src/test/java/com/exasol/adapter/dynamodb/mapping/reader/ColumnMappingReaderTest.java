package com.exasol.adapter.dynamodb.mapping.reader;

import static com.exasol.adapter.dynamodb.mapping.MappingErrorBehaviour.NULL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.MappingErrorBehaviour;
import com.exasol.adapter.dynamodb.mapping.PropertyToJsonColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToVarcharColumnMapping;
import com.exasol.adapter.dynamodb.mapping.TruncateableMappingErrorBehaviour;

class ColumnMappingReaderTest {

    /**
     * Tests for the {@link ColumnMappingReader} uses the correct default values for the
     * {@link PropertyToVarcharColumnMapping}.
     */
    @Test
    void testToStringColumnDefaultValues() {
        assertToStringDefinitionDefaultValues(Json.createObjectBuilder().build());
    }

    @Test
    void testToStringColumnExplicitDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("varcharColumnSize", 254)//
                .add("overflowBehaviour", "TRUNCATE")//
                .add("destinationName", "test")//
                .add("required", false).build();
        assertToStringDefinitionDefaultValues(definition);
    }

    void assertToStringDefinitionDefaultValues(final JsonObject definition) {
        final PropertyToVarcharColumnMapping columnMapping = (PropertyToVarcharColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toVarcharMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getVarcharColumnSize(), equalTo(254)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(TruncateableMappingErrorBehaviour.TRUNCATE)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getMappingErrorBehaviour(), equalTo(MappingErrorBehaviour.NULL)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(254) UTF8"))//
        );
    }

    @Test
    void testToStringColumnExplicitNonDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("varcharColumnSize", 123)//
                .add("overflowBehaviour", "ABORT")//
                .add("destinationName", "my_column")//
                .add("required", true).build();
        final PropertyToVarcharColumnMapping columnMapping = (PropertyToVarcharColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toVarcharMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getVarcharColumnSize(), equalTo(123)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(TruncateableMappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("MY_COLUMN")),
                () -> assertThat(columnMapping.getMappingErrorBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(123) UTF8"))//
        );
    }

    @Test
    void testUnsupportedMappingType() {
        final DocumentPathExpression.Builder sourcePath = DocumentPathExpression.builder();
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
                .add("varcharColumnSize", 254)//
                .add("overflowBehaviour", "TRUNCATE")//
                .add("destinationName", "test")//
                .add("required", false).build();
        assertToJsonDefinitionDefaultValues(definition);
    }

    void assertToJsonDefinitionDefaultValues(final JsonObject definition) {
        final PropertyToJsonColumnMapping columnMapping = (PropertyToJsonColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toJsonMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getVarcharColumnSize(), equalTo(254)),
                () -> assertThat(columnMapping.getOverflowBehaviour(), equalTo(NULL)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getMappingErrorBehaviour(), equalTo(MappingErrorBehaviour.NULL)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(254) UTF8"))//
        );
    }

    @Test
    void testToJsonColumnExplicitNonDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("varcharColumnSize", 123)//
                .add("overflowBehaviour", "ABORT")//
                .add("destinationName", "my_column")//
                .add("required", true).build();
        final PropertyToJsonColumnMapping columnMapping = (PropertyToJsonColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toJsonMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getVarcharColumnSize(), equalTo(123)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("MY_COLUMN")),
                () -> assertThat(columnMapping.getMappingErrorBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(123) UTF8"))//
        );
    }
}