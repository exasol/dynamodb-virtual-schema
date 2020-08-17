package com.exasol.adapter.document.mapping.reader;

import static com.exasol.adapter.document.mapping.MappingErrorBehaviour.ABORT;
import static com.exasol.adapter.document.mapping.MappingErrorBehaviour.NULL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.*;

class ColumnMappingReaderTest {

    /**
     * Tests for the {@link ColumnMappingReader} uses the correct default values for the
     * {@link PropertyToVarcharColumnMapping}.
     */
    @Test
    void testToVarcharColumnDefaultValues() {
        assertToVarcharDefinitionDefaultValues(Json.createObjectBuilder().build());
    }

    @Test
    void testToVarcharColumnExplicitDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("varcharColumnSize", 254)//
                .add("overflowBehaviour", "TRUNCATE")//
                .add("destinationName", "test")//
                .add("required", false).build();
        assertToVarcharDefinitionDefaultValues(definition);
    }

    void assertToVarcharDefinitionDefaultValues(final JsonObject definition) {
        final PropertyToVarcharColumnMapping columnMapping = (PropertyToVarcharColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toVarcharMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getVarcharColumnSize(), equalTo(254)),
                () -> assertThat(columnMapping.getOverflowBehaviour(),
                        equalTo(TruncateableMappingErrorBehaviour.TRUNCATE)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.NULL)),
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
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
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
                .add("overflowBehaviour", "ABORT")//
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
                () -> assertThat(columnMapping.getOverflowBehaviour(), equalTo(ABORT)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.NULL)),
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
                () -> assertThat(columnMapping.getOverflowBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("MY_COLUMN")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("VARCHAR(123) UTF8"))//
        );
    }

    @Test
    void testToDecimalColumnDefaultValues() {
        assertToDecimalMapping(Json.createObjectBuilder().build());
    }

    @Test
    void testToDecimalColumnExplicitDefaultValues() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("overflowBehaviour", "ABORT")//
                .add("notNumericBehaviour", "ABORT")//
                .add("decimalPrecision", 18)//
                .add("decimalScale", 0)//
                .add("required", false).build();
        assertToDecimalMapping(definition);
    }

    void assertToDecimalMapping(final JsonObject definition) {
        final PropertyToDecimalColumnMapping columnMapping = (PropertyToDecimalColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toDecimalMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getOverflowBehaviour(), equalTo(ABORT)),
                () -> assertThat(columnMapping.getNotNumericBehaviour(), equalTo(ABORT)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("TEST")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(NULL)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("DECIMAL(18, 0)"))//
        );
    }

    @Test
    void testToDecimalMappingWithNonDefaults() {
        final JsonObject definition = Json.createObjectBuilder()//
                .add("overflowBehaviour", "NULL")//
                .add("notNumericBehaviour", "NULL")//
                .add("decimalPrecision", 10)//
                .add("decimalScale", 5)//
                .add("destinationName", "my_column")//
                .add("required", true).build();
        final PropertyToDecimalColumnMapping columnMapping = (PropertyToDecimalColumnMapping) ColumnMappingReader
                .getInstance()
                .readColumnMapping("toDecimalMapping", definition, DocumentPathExpression.builder(), "test", false);
        assertAll(//
                () -> assertThat(columnMapping.getOverflowBehaviour(), equalTo(NULL)),
                () -> assertThat(columnMapping.getNotNumericBehaviour(), equalTo(NULL)),
                () -> assertThat(columnMapping.getExasolColumnName(), equalTo("MY_COLUMN")),
                () -> assertThat(columnMapping.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(columnMapping.getExasolDataType().toString(), equalTo("DECIMAL(10, 5)"))//
        );
    }
}