package com.exasol.adapter.dynamodb.mapping.reader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.*;

@Tag("integration")
@Tag("quick")
class JsonSchemaMappingReaderIT {
    private final MappingTestFiles mappingTestFiles = new MappingTestFiles();

    private SchemaMapping getMappingDefinitionForFile(final File mappingFile) throws IOException, AdapterException {
        final SchemaMappingReader mappingFactory = new JsonSchemaMappingReader(mappingFile,
                (tableName, mappedColumns) -> {
                    final List<ColumnMapping> key = mappedColumns.stream()
                            .filter(column -> column.getExasolColumnName().equals("ISBN")).collect(Collectors.toList());
                    if (key.isEmpty()) {
                        throw new TableKeyFetcher.NoKeyFoundException();
                    }
                    return key;
                });
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
        final Map<String, String> columnNames = getColumnNamesWithType(columns);
        final PropertyToVarcharColumnMapping isbnColumn = (PropertyToVarcharColumnMapping) getColumnByExasolName(table,
                "ISBN");
        final PropertyToVarcharColumnMapping nameColumn = (PropertyToVarcharColumnMapping) getColumnByExasolName(table,
                "NAME");
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(table.getRemoteName(), equalTo("MY_BOOKS")),
                () -> assertThat(columnNames,
                        equalTo(Map.of("ISBN", "VARCHAR(20) UTF8", "NAME", "VARCHAR(100) UTF8", "AUTHOR_NAME",
                                "VARCHAR(20) UTF8", "PUBLISHER", "VARCHAR(100) UTF8", "PRICE", "DECIMAL(8, 2)"))),
                () -> assertThat(isbnColumn.getVarcharColumnSize(), equalTo(20)),
                () -> assertThat(isbnColumn.getOverflowBehaviour(), equalTo(TruncateableMappingErrorBehaviour.ABORT)),
                () -> assertThat(isbnColumn.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.ABORT)),
                () -> assertThat(nameColumn.getLookupFailBehaviour(), equalTo(MappingErrorBehaviour.NULL)),
                () -> assertThat(nameColumn.getOverflowBehaviour(),
                        equalTo(TruncateableMappingErrorBehaviour.TRUNCATE)));
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

    private List<String> getColumnNames(final List<ColumnMapping> columns) {
        return columns.stream().map(ColumnMapping::getExasolColumnName).collect(Collectors.toList());
    }

    private Map<String, String> getColumnNamesWithType(final List<ColumnMapping> columns) {
        return columns.stream().collect(
                Collectors.toMap(ColumnMapping::getExasolColumnName, column -> column.getExasolDataType().toString()));
    }

    @Test
    void testToSingleColumnTableMapping() throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(
                MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping nestedTable = tables.stream().filter(table -> !table.isRootTable()).findAny().orElseThrow();
        final PropertyToVarcharColumnMapping column = (PropertyToVarcharColumnMapping) getColumnByExasolName(
                nestedTable, "NAME");
        assertAll(//
                () -> assertThat(tables.size(), equalTo(2)),
                () -> assertThat(nestedTable.getExasolName(), equalTo("BOOKS_TOPICS")),
                () -> assertThat(getColumnNames(nestedTable.getColumns()), containsInAnyOrder("BOOKS_ISBN", "NAME")),
                () -> assertThat(column.getPathToSourceProperty().toString(), equalTo("/topics[*]"))//
        );
    }

    @Test
    void testToStringMappingAtRootLevelException() throws IOException {
        final File invalidFile = this.mappingTestFiles.generateInvalidFile(MappingTestFiles.BASIC_MAPPING_FILE,
                base -> {
                    final JSONObject newMappings = new JSONObject();
                    newMappings.put("toVarcharMapping", new JSONObject());
                    base.put("mapping", newMappings);
                    return base;
                });

        final ExasolDocumentMappingLanguageException exception = assertThrows(
                ExasolDocumentMappingLanguageException.class, () -> getMappingDefinitionForFile(invalidFile));
        assertThat(exception.getMessage(), startsWith(
                "toVarcharMapping is not allowed at root level. You probably want to replace it with a \"fields\" definition. In mapping definition file"));
    }

    @Test
    void testDifferentKeysException() throws IOException {
        final File invalidFile = this.mappingTestFiles.generateInvalidFile(MappingTestFiles.BASIC_MAPPING_FILE,
                base -> {
                    base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("name")
                            .getJSONObject("toVarcharMapping").put("key", "local");
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
                            .getJSONObject("toVarcharMapping").put("key", "local");
                    return base;
                });
        final ExasolDocumentMappingLanguageException exception = assertThrows(
                ExasolDocumentMappingLanguageException.class, () -> getMappingDefinitionForFile(invalidFile));
        assertThat(exception.getMessage(),
                startsWith("Local keys make no sense in root table mapping definitions. Please make this key global."));
    }

    @Test
    void testNestedTableRootKeyGeneration() throws IOException, AdapterException {
        final File mappingFile = this.mappingTestFiles
                .generateInvalidFile(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE, base -> {
                    base.getJSONObject("mapping").getJSONObject("fields").getJSONObject("isbn")
                            .getJSONObject("toVarcharMapping").remove("key");
                    return base;
                });
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(mappingFile);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping nestedTable = tables.stream().filter(table -> !table.isRootTable()).findAny().orElseThrow();
        assertThat(getColumnNames(nestedTable.getColumns()), containsInAnyOrder("NAME", "BOOKS_ISBN"));
    }

    @Test
    void testNestedTableRootKeyGenerationException() throws IOException, AdapterException {
        final File mappingFile = this.mappingTestFiles
                .generateInvalidFile(MappingTestFiles.SINGLE_COLUMN_TO_TABLE_MAPPING_FILE, base -> {
                    base.getJSONObject("mapping").getJSONObject("fields").remove("isbn");
                    return base;
                });
        final ExasolDocumentMappingLanguageException exception = assertThrows(
                ExasolDocumentMappingLanguageException.class, () -> getMappingDefinitionForFile(mappingFile));
        assertThat(exception.getMessage(), startsWith(
                "Could not infer keys for table BOOKS. Please define a unique key by setting key='global' for one or more columns."));
    }

    @Test
    void testDoubleNestedToTableMapping() throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getMappingDefinitionForFile(
                MappingTestFiles.DOUBLE_NESTED_TO_TABLE_MAPPING_FILE);
        final List<TableMapping> tables = schemaMapping.getTableMappings();
        final TableMapping doubleNestedTable = tables.stream()
                .filter(table -> table.getExasolName().equals("BOOKS_CHAPTERS_FIGURES")).findAny().orElseThrow();
        final TableMapping nestedTable = tables.stream().filter(table -> table.getExasolName().equals("BOOKS_CHAPTERS"))
                .findAny().orElseThrow();
        final TableMapping rootTable = tables.stream().filter(table -> table.getExasolName().equals("BOOKS")).findAny()
                .orElseThrow();
        final PropertyToColumnMapping foreignKey1 = (PropertyToColumnMapping) getColumnByExasolName(nestedTable,
                "BOOKS_ISBN");
        final IterationIndexColumnMapping indexColumn = (IterationIndexColumnMapping) getColumnByExasolName(nestedTable,
                "INDEX");
        final PropertyToVarcharColumnMapping figureNameColumn = (PropertyToVarcharColumnMapping) getColumnByExasolName(
                doubleNestedTable, "NAME");
        assertAll(//
                () -> assertThat(tables.size(), equalTo(3)),
                () -> assertThat(getColumnNames(rootTable.getColumns()), containsInAnyOrder("ISBN", "NAME")),
                () -> assertThat(getColumnNames(nestedTable.getColumns()),
                        containsInAnyOrder("BOOKS_ISBN", "INDEX", "NAME")),
                () -> assertThat(getColumnNames(doubleNestedTable.getColumns()),
                        containsInAnyOrder("BOOKS_ISBN", "BOOKS_CHAPTERS_INDEX", "NAME")),
                () -> assertThat(figureNameColumn.getPathToSourceProperty().toString(),
                        equalTo("/chapters[*]/figures[*]/name")),
                () -> assertThat(foreignKey1.getPathToSourceProperty().toString(), equalTo("/isbn")), //
                () -> assertThat(indexColumn.getTablesPath().toString(), equalTo("/chapters[*]")));
    }

    private ColumnMapping getColumnByExasolName(final TableMapping table, final String exasolName) {
        return table.getColumns().stream().filter(each -> each.getExasolColumnName().equals(exasolName)).findAny()
                .orElseThrow();
    }
}
