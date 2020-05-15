package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;

public class JsonMappingReaderTest {
    private final MappingTestFiles mappingTestFiles = new MappingTestFiles();

    private SchemaMapping getMappingDefinitionForFile(final File mappingFile) throws IOException, AdapterException {
        final SchemaMappingReader mappingFactory = new JsonSchemaMappingReader(mappingFile);
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
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(MappingTestFiles.BASIC_MAPPING_FILE);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping table = tables.get(0);
        final List<ColumnMapping> columns = table.getColumns();
        final List<String> columnNames = getColumnNames(columns);
        final ToStringPropertyToColumnMapping isbnColumn = (ToStringPropertyToColumnMapping) getColumnByExasolName(
                table, "ISBN");
        final ToStringPropertyToColumnMapping nameColumn = (ToStringPropertyToColumnMapping) getColumnByExasolName(
                table, "NAME");
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(table.getRemoteName(), equalTo("MY_BOOKS")),
                () -> assertThat(columnNames, containsInAnyOrder("ISBN", "NAME", "AUTHOR_NAME", "PUBLISHER", "PRICE")),
                () -> assertThat(isbnColumn.getExasolStringSize(), equalTo(20)),
                () -> assertThat(isbnColumn.getOverflowBehaviour(),
                        equalTo(ToStringPropertyToColumnMapping.OverflowBehaviour.EXCEPTION)),
                () -> assertThat(isbnColumn.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.EXCEPTION)),
                () -> assertThat(nameColumn.getLookupFailBehaviour(), equalTo(LookupFailBehaviour.DEFAULT_VALUE)),
                () -> assertThat(nameColumn.getExasolStringSize(), equalTo(100)),
                () -> assertThat(nameColumn.getOverflowBehaviour(),
                        equalTo(ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE)));
    }

    @Test
    void testToJsonMapping() throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(MappingTestFiles.TO_JSON_MAPPING_FILE);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping table = tables.get(0);
        final List<ColumnMapping> columns = table.getColumns();
        final List<String> columnNames = getColumnNames(columns);
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(columnNames, containsInAnyOrder("ISBN", "NAME", "TOPICS")));
    }

    @NotNull
    private List<String> getColumnNames(final List<ColumnMapping> columns) {
        return columns.stream().map(ColumnMapping::getExasolColumnName).collect(Collectors.toList());
    }

    @Test
    void testToSingleColumnTableMapping() throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(
                MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping nestedTable = tables.stream().filter(table -> !table.isRootTable()).findAny().get();
        final ToStringPropertyToColumnMapping column = (ToStringPropertyToColumnMapping) getColumnByExasolName(
                nestedTable, "TOPIC_NAME");
        assertAll(//
                () -> assertThat(tables.size(), equalTo(2)),
                () -> assertThat(nestedTable.getExasolName(), equalTo("BOOKS_TOPICS")),
                () -> assertThat(getColumnNames(nestedTable.getColumns()),
                        containsInAnyOrder("BOOKS_ISBN", "TOPIC_NAME")),
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

    @Test
    void testLocalKeyAtRootLevelException() throws IOException {
        final File invalidFile = this.mappingTestFiles
                .generateInvalidFile(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE, base -> {
                    base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("isbn")
                            .getJSONObject("toStringMapping").put("key", "local");
                    return base;
                });
        final ExasolDocumentMappingLanguageException exception = assertThrows(
                ExasolDocumentMappingLanguageException.class, () -> getMappingDefinitionForFile(invalidFile));
        assertThat(exception.getMessage(),
                startsWith("Local keys make no sense in root table mapping definitions. Please make this key global."));
    }

    @Test
    void testDoubleNestedToTableMapping() throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(
                MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING_FILE);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping doubleNestedTable = tables.stream()
                .filter(table -> table.getExasolName().equals("BOOKS_CHAPTERS_FIGURES")).findAny().get();
        final TableMapping nestedTable = tables.stream().filter(table -> table.getExasolName().equals("BOOKS_CHAPTERS"))
                .findAny().get();
        final PropertyToColumnMapping foreignKey1 = (PropertyToColumnMapping) getColumnByExasolName(nestedTable,
                "BOOKS_ISBN");
        final IterationIndexColumnMapping indexColumn = (IterationIndexColumnMapping) getColumnByExasolName(nestedTable,
                "INDEX");
        final ToStringPropertyToColumnMapping figureNameColumn = (ToStringPropertyToColumnMapping) getColumnByExasolName(
                doubleNestedTable, "NAME");
        assertAll(//
                () -> assertThat(tables.size(), equalTo(3)),
                () -> assertThat(getColumnNames(nestedTable.getColumns()), containsInAnyOrder("BOOKS_ISBN", "INDEX")),
                () -> assertThat(getColumnNames(doubleNestedTable.getColumns()),
                        containsInAnyOrder("BOOKS_ISBN", "BOOKS_CHAPTERS_INDEX", "NAME")),
                () -> assertThat(figureNameColumn.getPathToSourceProperty().toString(),
                        equalTo("/chapters[*]/figures[*]")),
                () -> assertThat(foreignKey1.getPathToSourceProperty().toString(), equalTo("/isbn")), //
                () -> assertThat(indexColumn.getTablesPath().toString(), equalTo("/chapters[*]")));
    }

    private ColumnMapping getColumnByExasolName(final TableMapping table, final String exasolName) {
        return table.getColumns().stream().filter(each -> each.getExasolColumnName().equals(exasolName)).findAny()
                .get();
    }
}
