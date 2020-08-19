package com.exasol.adapter.document.mapping.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.mapping.SchemaMapping;
import com.exasol.adapter.document.mapping.TableKeyFetcher;
import com.exasol.adapter.document.mapping.TableMapping;

/**
 * This class reads a {@link SchemaMapping} from JSON files.
 * <p>
 * The JSON files must follow the schema defined in {@code resources/schemas/edml/v1.json}. Documentation of schema
 * mapping definitions can be found at {@code /doc/gettingStartedWithSchemaMappingLanguage.md}.
 * </p>
 */
public class JsonSchemaMappingReader implements SchemaMappingReader {
    private final List<TableMapping> tables = new ArrayList<>();
    private final TableKeyFetcher tableKeyFetcher;

    /**
     * Create an instance of {@link JsonSchemaMappingReader}.
     *
     * @param definitionsPath path to the definition. Can either be a {@code .json} file or an directory. If it points
     *                        to a directory, all {@code .json} files are loaded.
     * @param tableKeyFetcher remote database specific {@link TableKeyFetcher}
     * @throws IOException                            if could not open file
     * @throws AdapterException                       if schema mapping no mapping files were found
     * @throws ExasolDocumentMappingLanguageException if schema mapping invalid
     */
    public JsonSchemaMappingReader(final File definitionsPath, final TableKeyFetcher tableKeyFetcher)
            throws IOException, AdapterException {
        this(splitIfDirectory(definitionsPath), tableKeyFetcher);
    }

    private JsonSchemaMappingReader(final File[] definitionsPaths, final TableKeyFetcher tableKeyFetcher)
            throws IOException {
        this.tableKeyFetcher = tableKeyFetcher;
        final JsonSchemaMappingValidator jsonSchemaMappingValidator = new JsonSchemaMappingValidator();
        for (final File definitionPath : definitionsPaths) {
            jsonSchemaMappingValidator.validate(definitionPath);
            try {
                parseFile(definitionPath);
            } catch (final ExasolDocumentMappingLanguageException exception) {
                throw new ExasolDocumentMappingLanguageException(
                        exception.getMessage() + " In mapping definition file " + definitionPath.toString());
            }
        }
    }

    /**
     * If the given definitionsPath is an directory all json files are returned.
     *
     * @param definitionsPath path to file or directory
     * @return array of definition files
     */
    private static File[] splitIfDirectory(final File definitionsPath) throws AdapterException {
        if (definitionsPath.isFile()) {
            return new File[] { definitionsPath };
        } else {
            return splitDirectory(definitionsPath);
        }
    }

    private static File[] splitDirectory(final File definitionsPath) throws AdapterException {
        final String jsonFileEnding = ".json";
        final File[] files = definitionsPath.listFiles((file, fileName) -> fileName.endsWith(jsonFileEnding));
        if (files == null || files.length == 0) {
            throw new AdapterException("No schema mapping files found in " + definitionsPath
                    + ". Please check that you definition files have a .json ending and are uploaded to the BucketFS path that was specified in the MAPPING property.");
        }
        return files;
    }

    private void parseFile(final File definitionPath) throws IOException {
        try (final InputStream inputStream = new FileInputStream(definitionPath);
                final JsonReader reader = Json.createReader(inputStream)) {
            final JsonObject definitionObject = reader.readObject();
            this.tables.addAll(new RootTableMappingReader(definitionObject, this.tableKeyFetcher).getTables());
        }
    }

    @Override
    public SchemaMapping getSchemaMapping() {
        return new SchemaMapping(this.tables);
    }
}
