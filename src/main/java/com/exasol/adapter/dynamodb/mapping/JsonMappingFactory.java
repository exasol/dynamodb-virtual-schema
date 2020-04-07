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
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tostringmapping.ToStringColumnMappingDefinition;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalkerBuilder;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;

/**
 * This {@link MappingDefinitionFactory} reads a {@link SchemaMappingDefinition} from JSON files.
 * <p>
 * The JSON files must follow the schema defined at {@code resources/mappingLanguageSchema.json}. Documentation of
 * schema mapping definitions can be found at {@code /doc/gettingStartedWithSchemaMappingLanguage.md}.
 * </p>
 */
public class JsonMappingFactory implements MappingDefinitionFactory {
    private static final String DEST_TABLE_NAME_KEY = "destTable";
    private static final String MAPPING_KEY = "mapping";
    private static final String FIELDS_KEY = "fields";
    private static final String TO_STRING_MAPPING_KEY = "toStringMapping";
    private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";
    private static final String MAX_LENGTH_KEY = "maxLength";
    private static final int DEFAULT_MAX_LENGTH = 254;
    private static final String OVERFLOW_KEY = "overflow";
    private static final String DEST_NAME_KEY = "destName";
    private static final String REQUIRED_KEY = "required";
    private static final ToStringColumnMappingDefinition.OverflowBehaviour DEFAULT_TO_STRING_OVERFLOW = ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE;
    private static final AbstractColumnMappingDefinition.LookupFailBehaviour DEFAULT_LOOKUP_BEHAVIOUR = AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE;

    private final List<TableMappingDefinition> tables = new ArrayList<>();

    /**
     * Creates an instance of {@link JsonMappingFactory}.
     * 
     * @param definitionsPath path to the definition. Can either be a {@code .json} file or an directory. If it points
     *                        to an directory, all {@code .json} files are loaded.
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
        final String jsonFileEnding = ".json";
        if (definitionsPath.isFile()) {
            return new File[] { definitionsPath };
        } else {
            final File[] files = definitionsPath.listFiles((file, fileName) -> fileName.endsWith(jsonFileEnding));
            if (files == null || files.length == 0) {
                throw new AdapterException("no schema mapping files found in " + definitionsPath);
            }
            return files;
        }
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

        switch (getMappingType(definition)) {
        case TO_STRING_MAPPING_KEY:
            if (isRootLevel) {
                throw new MappingException("ToString mapping is not allowed at root level");
            }
            addStringColumn(definition.getJsonObject(TO_STRING_MAPPING_KEY), walkerToThisPath, tableBuilder,
                    propertyName);
            break;
        case TO_JSON_MAPPING_KEY:
            addToJsonColumn(definition.getJsonObject(TO_JSON_MAPPING_KEY), walkerToThisPath, tableBuilder,
                    propertyName);
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
            throw new MappingException("It's not allowed to define more than one mapping for one property.");
        }
    }

    private void visitChildren(final JsonObject definition, final AbstractDynamodbResultWalkerBuilder walkerToThisPath,
            final TableMappingDefinition.Builder tableBuilder) throws MappingException {
        for (final String dynamodbPropertyName : definition.keySet()) {
            final ObjectDynamodbResultWalker.Builder walker = new ObjectDynamodbResultWalker.Builder(walkerToThisPath,
                    dynamodbPropertyName);
            this.visitMapping(definition.getJsonObject(dynamodbPropertyName), walker, tableBuilder,
                    dynamodbPropertyName, false);
        }

    }

    private void addStringColumn(final JsonObject definition, final AbstractDynamodbResultWalkerBuilder resultWalker,
            final TableMappingDefinition.Builder tableBuilder, final String dynamodbPropertyName)
            throws MappingException {
        final int maxLength = definition.getInt(MAX_LENGTH_KEY, DEFAULT_MAX_LENGTH);
        final ToStringColumnMappingDefinition.OverflowBehaviour overflowBehaviour = readStringOverflowBehaviour(
                definition);
        final AbstractColumnMappingDefinition.ConstructorParameters columnParameters = readColumnProperties(definition,
                resultWalker, dynamodbPropertyName);
        tableBuilder.withColumnMappingDefinition(
                new ToStringColumnMappingDefinition(columnParameters, maxLength, overflowBehaviour));
    }

    private AbstractColumnMappingDefinition.ConstructorParameters readColumnProperties(final JsonObject definition,
            final AbstractDynamodbResultWalkerBuilder resultWalker, final String dynamodbPropertyName)
            throws MappingException {
        final String destinationColumnName = readDestinationColumnName(definition, dynamodbPropertyName);
        final AbstractColumnMappingDefinition.LookupFailBehaviour lookupFailBehaviour = readLookupFailBehaviour(
                definition);
        return new AbstractColumnMappingDefinition.ConstructorParameters(destinationColumnName, resultWalker.build(),
                lookupFailBehaviour);
    }

    private ToStringColumnMappingDefinition.OverflowBehaviour readStringOverflowBehaviour(final JsonObject definition) {
        if (definition.containsKey(OVERFLOW_KEY) && definition.getString(OVERFLOW_KEY).equals("ABORT")) {
            return ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION;
        } else {
            return DEFAULT_TO_STRING_OVERFLOW;
        }
    }

    private AbstractColumnMappingDefinition.LookupFailBehaviour readLookupFailBehaviour(final JsonObject definition) {
        if (definition.containsKey(REQUIRED_KEY) && definition.getBoolean(REQUIRED_KEY)) {
            return AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION;
        } else {
            return DEFAULT_LOOKUP_BEHAVIOUR;
        }
    }

    private String readDestinationColumnName(final JsonObject definition, final String defaultValue)
            throws MappingException {
        final String destinationColumnName = definition.getString(DEST_NAME_KEY, defaultValue);
        if (destinationColumnName == null) {
            throw new MappingException("Please set " + DEST_NAME_KEY + " property");
        }
        return destinationColumnName.toUpperCase();
    }

    private void addToJsonColumn(final JsonObject definition, final AbstractDynamodbResultWalkerBuilder resultWalker,
            final TableMappingDefinition.Builder tableBuilder, final String dynamodbPropertyName)
            throws MappingException {
        final String destinationColumnName = readDestinationColumnName(definition, dynamodbPropertyName);
        final AbstractColumnMappingDefinition.ConstructorParameters columnParameters = new AbstractColumnMappingDefinition.ConstructorParameters(
                destinationColumnName, resultWalker.build(),
                AbstractColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE);
        tableBuilder.withColumnMappingDefinition(new ToJsonColumnMappingDefinition(columnParameters));
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
