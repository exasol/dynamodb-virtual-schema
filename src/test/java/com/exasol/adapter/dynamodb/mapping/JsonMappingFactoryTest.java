package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;

public class JsonMappingFactoryTest {
    private final MappingTestFiles mappingTestFiles = new MappingTestFiles();

    private SchemaMappingDefinition getMappingDefinitionForFile(final File mappingFile)
            throws IOException, AdapterException {
        final MappingDefinitionFactory mappingFactory = new JsonMappingFactory(mappingFile);
        return mappingFactory.getSchemaMapping();
    }

    @AfterEach
    void afterEach() {
        this.mappingTestFiles.deleteAllTempFiles();
    }

    /**
     * Tests schema load from basicMapping.json.
     */
    @Test
    void testBasicMapping() throws IOException, AdapterException {
        final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFile(MappingTestFiles.BASIC_MAPPING_FILE);
        final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
        final TableMappingDefinition table = tables.get(0);
        final List<ColumnMappingDefinition> columns = table.getColumns();
        final List<String> columnNames = columns.stream().map(ColumnMappingDefinition::getExasolColumnName)
                .collect(Collectors.toList());
        final ToStringColumnMappingDefinition isbnColumn = (ToStringColumnMappingDefinition) columns.stream()
                .filter(column -> column.getExasolColumnName().equals("ISBN")).findAny().get();
        final ToStringColumnMappingDefinition nameColumn = (ToStringColumnMappingDefinition) columns.stream()
                .filter(column -> column.getExasolColumnName().equals("NAME")).findAny().get();
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(table.getRemoteName(), equalTo("MY_BOOKS")),
                () -> assertThat(columnNames, containsInAnyOrder("ISBN", "NAME", "AUTHOR_NAME", "PUBLISHER", "PRICE")),
                () -> assertThat(isbnColumn.getExasolStringSize(), equalTo(20)),
                () -> assertThat(isbnColumn.getOverflowBehaviour(),
                        equalTo(ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION)),
                () -> assertThat(isbnColumn.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.EXCEPTION)),
                () -> assertThat(nameColumn.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.DEFAULT_VALUE)),
                () -> assertThat(nameColumn.getExasolStringSize(), equalTo(100)),
                () -> assertThat(nameColumn.getOverflowBehaviour(),
                        equalTo(ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE)));
    }

    @Test
    void testToJsonMapping() throws IOException, AdapterException {
        final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFile(
                MappingTestFiles.TO_JSON_MAPPING_FILE);
        final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
        final TableMappingDefinition table = tables.get(0);
        final List<ColumnMappingDefinition> columns = table.getColumns();
        final List<String> columnNames = columns.stream().map(ColumnMappingDefinition::getExasolColumnName)
                .collect(Collectors.toList());
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(columnNames, containsInAnyOrder("ISBN", "NAME", "TOPICS")));
    }

    @Test
    void testToSingleColumnTableMapping() throws IOException, AdapterException {
        final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFile(
                MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE);
        final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
        final TableMappingDefinition nestedTable = tables.stream().filter(table -> !table.isRootTable()).findAny()
                .get();
        final ToStringColumnMappingDefinition column = (ToStringColumnMappingDefinition) nestedTable.getColumns()
                .get(0);
        assertAll(//
                () -> assertThat(tables.size(), equalTo(2)),
                () -> assertThat(nestedTable.getExasolName(), equalTo("BOOKS_TOPICS")),
                () -> assertThat(column.getExasolColumnName(), equalTo("TOPIC_NAME")),
                () -> assertThat(column.getPathToSourceProperty().toString(), equalTo("/topics[*]"))//
        );
    }

    @Test
    void testToStringMappingAtRootLevelException() throws IOException {
        final File invalidFile = this.mappingTestFiles.generateInvalidFile(MappingTestFiles.BASIC_MAPPING_FILE,
                base -> {
                    final JSONObject newMappings = new JSONObject();
                    newMappings.put("toStringMapping", new JSONObject());
                    base.put("mapping", newMappings);
                    return base;
                });

        final ExasolDocumentMappingLanguageException exception = assertThrows(
                ExasolDocumentMappingLanguageException.class, () -> getMappingDefinitionForFile(invalidFile));
        assertThat(exception.getMessage(), startsWith(
                "ToStringMapping is not allowed at root level. You probably want to replace it with a \"fields\" definition. In mapping definition file"));
    }

    @Test
    void testDifferentKeysException() throws IOException {
        final File invalidFile = this.mappingTestFiles.generateInvalidFile(MappingTestFiles.BASIC_MAPPING_FILE,
                base -> {
                    base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("name")
                            .getJSONObject("toStringMapping").put("key", "local");
                    return base;
                });
        final ExasolDocumentMappingLanguageException exception = assertThrows(
                ExasolDocumentMappingLanguageException.class, () -> getMappingDefinitionForFile(invalidFile));
        assertThat(exception.getMessage(), startsWith(
                "/name: This table already has a key of different type (global/local). Please either define all keys of the table local or global."));
    }
}
