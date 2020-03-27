package com.exasol.adapter.dynamodb.mapping;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.exasol.adapter.AdapterException;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalkerBuilder;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.dynamodb.resultwalker.ObjectDynamodbResultWalker;

/**
 * This {@link MappingFactory} reads a {@link SchemaMappingDefinition} from JSON
 * files. The JSON files must follow the schema defined at
 * resources/mappingLanguageSchema.json. Documentation on schema mapping
 * definitions can be found at /doc/schemaMappingLanguageReference.md
 */
public class JsonMappingFactory implements MappingFactory {
	private static final String DEST_TABLE_NAME_KEY = "destTableName";
	private static final String MAPPING_KEY = "mapping";
	private static final String CHILDREN_KEY = "children";
	private static final String TO_STRING_MAPPING_KEY = "toStringMapping";
	private static final String TO_JSON_MAPPING_KEY = "toJsonMapping";
	private static final String MAX_LENGTH_KEY = "maxLength";
	private static final int DEFAULT_MAX_LENGTH = 254;
	private static final String OVERFLOW_KEY = "overflow";
	private static final String DEST_NAME_KEY = "destName";
	private static final String REQUIRED_KEY = "required";
	private static final ToStringColumnMappingDefinition.OverflowBehaviour DEFAULT_TO_STRING_OVERFLOW = ToStringColumnMappingDefinition.OverflowBehaviour.TRUNCATE;
	private static final ColumnMappingDefinition.LookupFailBehaviour DEFAULT_LOOKUP_BEHAVIOUR = ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE;

	private final List<TableMappingDefinition> tables = new ArrayList<>();

	/**
	 * Constructor.
	 * 
	 * @param definitionsPath
	 *            path to the definition. Can either be a .json file or an
	 *            directory. If it points to an directory, all .json files are
	 *            loaded.
	 * @throws IOException
	 *             if could not open file
	 * @throws SchemaMappingException
	 *             if schema mapping invalid
	 */
	public JsonMappingFactory(final File definitionsPath) throws IOException, SchemaMappingException {
		this(splitIfDirectory(definitionsPath));
	}

	private JsonMappingFactory(final File[] definitionsPaths) throws IOException, SchemaMappingException {
		final JsonMappingValidator jsonMappingValidator = new JsonMappingValidator();
		for (final File definitionPath : definitionsPaths) {
			try (final InputStream inputStream = new FileInputStream(definitionPath)) {
				final JSONObject definitionObject = new JSONObject(new JSONTokener(inputStream));
				jsonMappingValidator.validate(definitionObject);
				addRootDefinition(definitionObject);
			} catch (final MappingException e) {
				throw new SchemaMappingException(definitionPath.getName(), e);
			}
		}
	}

	/**
	 * If the given definitionsPath is an directory all json files are returned.
	 *
	 * @param definitionsPath
	 *            path to file or directory
	 * @return array of definition files
	 */
	private static File[] splitIfDirectory(final File definitionsPath) {
		final String jsonFileEnding = ".json";
		if (definitionsPath.isFile()) {
			return new File[]{definitionsPath};
		} else {
			return definitionsPath.listFiles((file, fileName) -> fileName.endsWith(jsonFileEnding));
		}
	}

	private void addRootDefinition(final JSONObject definition) throws MappingException {
		final TableMappingDefinition.Builder tableBuilder = TableMappingDefinition
				.builder(definition.getString(DEST_TABLE_NAME_KEY), true);
		walkRootMapping(definition.getJSONObject(MAPPING_KEY), new IdentityDynamodbResultWalker.Builder(),
				tableBuilder);

		this.tables.add(tableBuilder.build());
	}

	private void walkRootMapping(final JSONObject definition, final DynamodbResultWalkerBuilder walkerToThisPath,
			final TableMappingDefinition.Builder tableBuilder) throws MappingException {
		walkMapping(definition, walkerToThisPath, tableBuilder, null, true);
	}

	private void walkMapping(final JSONObject definition, final DynamodbResultWalkerBuilder walkerToThisPath,
			final TableMappingDefinition.Builder tableBuilder, final String propertyName, final boolean isRootLevel)
			throws MappingException {
		if (definition.has(TO_STRING_MAPPING_KEY)) {
			if (isRootLevel) {
				throw new MappingException("ToString mapping is not allowed at root level");
			}
			addStringColumn(definition.getJSONObject(TO_STRING_MAPPING_KEY), walkerToThisPath, tableBuilder,
					propertyName);
		} else if (definition.has(TO_JSON_MAPPING_KEY)) {
			addToJsonColumn(definition.getJSONObject(TO_JSON_MAPPING_KEY), walkerToThisPath, tableBuilder,
					propertyName);
		} else if (definition.has(CHILDREN_KEY)) {
			walkChildren(definition.getJSONObject(CHILDREN_KEY), walkerToThisPath, tableBuilder);
		} else {
			throw new UnsupportedOperationException("not yet implemented");
		}
	}

	private void walkChildren(final JSONObject definition, final DynamodbResultWalkerBuilder walkerToThisPath,
			final TableMappingDefinition.Builder tableBuilder) throws MappingException {
		for (final String dynamodbPropertyName : definition.keySet()) {
			final ObjectDynamodbResultWalker.Builder walker = new ObjectDynamodbResultWalker.Builder(walkerToThisPath,
					dynamodbPropertyName);
			this.walkMapping(definition.getJSONObject(dynamodbPropertyName), walker, tableBuilder, dynamodbPropertyName,
					false);
		}

	}

	private void addStringColumn(final JSONObject definition, final DynamodbResultWalkerBuilder resultWalker,
			final TableMappingDefinition.Builder tableBuilder, final String dynamodbPropertyName) {
		String destinationColumnName = dynamodbPropertyName;
		int maxLength = DEFAULT_MAX_LENGTH;
		ToStringColumnMappingDefinition.OverflowBehaviour overflowBehaviour = DEFAULT_TO_STRING_OVERFLOW;
		ColumnMappingDefinition.LookupFailBehaviour lookupFailBehaviour = DEFAULT_LOOKUP_BEHAVIOUR;
		if (definition.has(DEST_NAME_KEY)) {
			destinationColumnName = definition.getString(DEST_NAME_KEY);
		}
		if (definition.has(MAX_LENGTH_KEY)) {
			maxLength = definition.getInt(MAX_LENGTH_KEY);
		}
		if (definition.has(OVERFLOW_KEY) && definition.getString(OVERFLOW_KEY).equals("ABORT")) {
			overflowBehaviour = ToStringColumnMappingDefinition.OverflowBehaviour.EXCEPTION;
		}
		if (definition.has(REQUIRED_KEY) && definition.getBoolean(REQUIRED_KEY)) {
			lookupFailBehaviour = ColumnMappingDefinition.LookupFailBehaviour.EXCEPTION;
		}

		tableBuilder.withColumnMappingDefinition(new ToStringColumnMappingDefinition(destinationColumnName, maxLength,
				resultWalker.build(), lookupFailBehaviour, overflowBehaviour));
	}

	private void addToJsonColumn(final JSONObject definition, final DynamodbResultWalkerBuilder resultWalker,
			final TableMappingDefinition.Builder tableBuilder, final String dynamodbPropertyName)
			throws MappingException {
		String destinationColumnName = dynamodbPropertyName;
		if (definition.has(DEST_NAME_KEY)) {
			destinationColumnName = definition.getString(DEST_NAME_KEY);
		}
		if (destinationColumnName == null) {
			throw new MappingException(String.format("please set %s property", DEST_NAME_KEY));
		}
		tableBuilder.withColumnMappingDefinition(new ToJsonColumnMappingDefinition(destinationColumnName,
				resultWalker.build(), ColumnMappingDefinition.LookupFailBehaviour.DEFAULT_VALUE));
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
		 * Constructor.
		 * 
		 * @param causingMappingDefinitionFileName
		 *            mapping definition file that contains the mistake
		 * @param mappingException
		 *            causing {@link MappingException}
		 */
		public SchemaMappingException(final String causingMappingDefinitionFileName,
				final MappingException mappingException) {
			super(String.format("Error in schema mapping %s:", causingMappingDefinitionFileName), mappingException);
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
	 * Exception that is thrown on mapping failures. This exceptions are caught and
	 * encapsulated into a {@link SchemaMappingException} with additional info for
	 * the causing definition file.
	 */
	public static class MappingException extends AdapterException {
		/**
		 * Constructor.
		 * 
		 * @param message
		 *            Exception message
		 */
		public MappingException(final String message) {
			super(message);
		}
	}
}
