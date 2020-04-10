package com.exasol.adapter.dynamodb.mapping;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.exasol.adapter.AdapterException;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalkerBuilder;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;

/**
 * This {@link MappingDefinitionFactory} reads a {@link SchemaMappingDefinition} from JSON files.
 * <p>
 * The JSON files must follow the schema defined in {@code resources/mappingLanguageSchema.json}. Documentation of
 * schema mapping definitions can be found at {@code /doc/gettingStartedWithSchemaMappingLanguage.md}.
 * </p>
 */
public class JsonMappingFactory implements MappingDefinitionFactory {
    private static final String DEST_TABLE_NAME_KEY = "destTable";
    private static final String MAPPING_KEY = "mapping";
    private static final String FIELDS_KEY = "fields";
    private static final String TO_STRING_MAPPING_KEY = "toStringMapping";
    private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";

    private final List<TableMappingDefinition> tables = new ArrayList<>();

    /**
     * Creates an instance of {@link JsonMappingFactory}.
     * 
     * @param definitionsPath path to the definition. Can either be a {@code .json} file or an directory. If it points
     *                        to a directory, all {@code .json} files are loaded.
     * @throws IOException            if could not open file
     * @throws SchemaMappingException if schema mapping invalid
     */
    public JsonMappingFactory(final File definitionsPath) throws IOException, AdapterException {
        this(splitIfDirectory(definitionsPath));
    }

    private JsonMappingFactory(final File[] definitionsPaths) throws IOException, SchemaMappingException {
        final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
        for (final File definitionPath : definitionsPaths) {
            try {
                jsonMappingValidator.validate(definitionPath);
                parseFile(definitionPath);
            } catch (final MappingException exception) {
                throw new SchemaMappingException(definitionPath.getName(), exception);
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

    private void parseFile(final File definitionPath) throws IOException, MappingException {
        try (final InputStream inputStream = new FileInputStream(definitionPath);
                final JsonReader reader = Json.createReader(inputStream)) {
            final JsonObject definitionObject = reader.readObject();
            addRootDefinition(definitionObject);
        }
    }

    private void addRootDefinition(final JsonObject definition) throws MappingException {
        final TableMappingDefinition.Builder tableBuilder = TableMappingDefinition
                .builder(definition.getString(DEST_TABLE_NAME_KEY), true);
        visitRootMapping(definition.getJsonObject(MAPPING_KEY), new IdentityDynamodbResultWalker.Builder(),
                tableBuilder);
        this.tables.add(tableBuilder.build());
    }

    private void visitRootMapping(final JsonObject definition,
            final AbstractDynamodbResultWalkerBuilder walkerToThisPath,
            final TableMappingDefinition.Builder tableBuilder) throws MappingException {
        visitMapping(definition, walkerToThisPath, tableBuilder, null, true);
    }

    private void visitMapping(final JsonObject definition, final AbstractDynamodbResultWalkerBuilder walkerToThisPath,
            final TableMappingDefinition.Builder tableBuilder, final String propertyName, final boolean isRootLevel)
            throws MappingException {
        final JsonColumnMappingFactory columnMappingFactory = new JsonColumnMappingFactory();
        switch (getMappingType(definition)) {
        case TO_STRING_MAPPING_KEY:
            columnMappingFactory.addStringColumnIfPossible(definition.getJsonObject(TO_STRING_MAPPING_KEY),
                    walkerToThisPath, tableBuilder, propertyName, isRootLevel);
            break;
        case TO_JSON_MAPPING_KEY:
            columnMappingFactory.addToJsonColumn(definition.getJsonObject(TO_JSON_MAPPING_KEY), walkerToThisPath,
                    tableBuilder, propertyName);
            break;
        case FIELDS_KEY:
            visitChildren(definition.getJsonObject(FIELDS_KEY), walkerToThisPath, tableBuilder);
            break;
        case "":// no mapping definition
            break;
        default:
            throw new UnsupportedOperationException("This mapping type is not supported in the current version.");
        }
    }

    private String getMappingType(final JsonObject definition) throws MappingException {
        final Set<String> keys = definition.keySet();
        if (keys.isEmpty()) {
            return "";
        } else if (keys.size() == 1) {
            return keys.iterator().next();
        } else {
            throw new MappingException("Please, define only one mapping for one property.");
        }
    }

    private void visitChildren(final JsonObject definition, final AbstractDynamodbResultWalkerBuilder walkerToThisPath,
            final TableMappingDefinition.Builder tableBuilder) throws MappingException {
        for (final String dynamodbPropertyName : definition.keySet()) {
            final ObjectDynamodbResultWalker.Builder walker = new ObjectDynamodbResultWalker.Builder(walkerToThisPath,
                    dynamodbPropertyName);
            visitMapping(definition.getJsonObject(dynamodbPropertyName), walker, tableBuilder, dynamodbPropertyName,
                    false);
        }
    }

    @Override
    public SchemaMappingDefinition getSchemaMapping() {
        return new SchemaMappingDefinition(this.tables);
    }

    /**
     * Exception thrown if schema mapping definition is invalid.
     */
    public static class SchemaMappingException extends AdapterException {
        private final String causingMappingDefinitionFileName;

        /**
         * Creates an instance of {@link SchemaMappingException}.
         * 
         * @param causingMappingDefinitionFileName mapping definition file that contains the mistake
         * @param mappingException                 causing {@link MappingException}
         */
        public SchemaMappingException(final String causingMappingDefinitionFileName,
                final MappingException mappingException) {
            super("Error in schema mapping " + causingMappingDefinitionFileName + ":", mappingException);
            this.causingMappingDefinitionFileName = causingMappingDefinitionFileName;
        }

        /**
         * Gives the file name of the mapping definition that caused this exception.
         * 
         * @return file name
         */
        public String getCausingMappingDefinitionFileName() {
            return this.causingMappingDefinitionFileName;
        }
    }

    /**
     * Exception that is thrown on mapping failures. This exceptions are caught and encapsulated into a
     * {@link SchemaMappingException} with additional info for the causing definition file.
     */
    public static class MappingException extends AdapterException {
        /**
         * Creates an instance of {@link MappingException}.
         * 
         * @param message Exception message
         */
        public MappingException(final String message) {
            super(message);
        }
    }
}
