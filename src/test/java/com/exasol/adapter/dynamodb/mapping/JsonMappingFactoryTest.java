package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;

/**
 * Tests for {@link JsonMappingFactory}
 */
public class JsonMappingFactoryTest {

    private SchemaMappingDefinition getMappingDefinitionForFileName(final String name)
            throws IOException, AdapterException {
        final ClassLoader classLoader = JsonMappingFactory.class.getClassLoader();
        final MappingDefinitionFactory mappingFactory = new JsonMappingFactory(
                new File(classLoader.getResource(name).getFile()));
        return mappingFactory.getSchemaMapping();
    }

    /**
     * Tests schema load from basicMapping.json
     */
    @Test
    void testBasicMapping() throws IOException, AdapterException {
        final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("basicMapping.json");
        final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
        final TableMappingDefinition table = tables.get(0);
        final List<AbstractColumnMappingDefinition> columns = table.getColumns();
        final List<String> columnNames = columns.stream().map(AbstractColumnMappingDefinition::getExasolColumnName)
                .collect(Collectors.toList());
        final ToStringColumnMappingDefinition isbnColumn = (ToStringColumnMappingDefinition) columns.stream()
                .filter(column -> column.getExasolColumnName().equals("ISBN")).findAny().get();
        final ToStringColumnMappingDefinition nameColumn = (ToStringColumnMappingDefinition) columns.stream()
                .filter(column -> column.getExasolColumnName().equals("NAME")).findAny().get();
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(columnNames, containsInAnyOrder("ISBN", "NAME", "AUTHOR_NAME")),
                () -> assertThat(isbnColumn.getExasolStringSize(), equalTo(20)),
                () -> assertThat(isbnColumn.getOverflowBehaviour(),
                        equalTo(ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION)),
                () -> assertThat(isbnColumn.getLookupFailBehaviour(),
                        equalTo(AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION)),
                () -> assertThat(nameColumn.getLookupFailBehaviour(),
                        equalTo(AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE)),
                () -> assertThat(nameColumn.getExasolStringSize(), equalTo(100)),
                () -> assertThat(nameColumn.getOverflowBehaviour(),
                        equalTo(ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE)));
    }

    @Test
    void testToJsonMapping() throws IOException, AdapterException {
        final SchemaMappingDefinition schemaMapping = getMappingDefinitionForFileName("toJsonMapping.json");
        final List<TableMappingDefinition> tables = schemaMapping.getTableMappings();
        final TableMappingDefinition table = tables.get(0);
        final List<AbstractColumnMappingDefinition> columns = table.getColumns();
        final List<String> columnNames = columns.stream().map(AbstractColumnMappingDefinition::getExasolColumnName)
                .collect(Collectors.toList());
        assertAll(() -> assertThat(tables.size(), equalTo(1)), //
                () -> assertThat(table.getExasolName(), equalTo("BOOKS")),
                () -> assertThat(columnNames, containsInAnyOrder("ISBN", "NAME", "TOPICS")));
    }

    @Test
    void testException() {
        final String fileName = "invalidToStringMappingAtRootLevel.json";
        final JsonMappingFactory.SchemaMappingException exception = assertThrows(
                JsonMappingFactory.SchemaMappingException.class, () -> getMappingDefinitionForFileName(fileName));
        assertAll(() -> assertThat(exception.getCausingMappingDefinitionFileName(), equalTo(fileName)),
                () -> assertThat(exception.getMessage(),
                        equalTo("Error in schema mapping invalidToStringMappingAtRootLevel.json:")),
                () -> assertThat(exception.getCause().getMessage(), equalTo(
                        "ToStringMapping is not allowed at root level. You probably want to replace it with a \"fields\" definition.")));
    }
}
