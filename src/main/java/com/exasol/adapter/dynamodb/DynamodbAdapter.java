package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dynamodb.mapping.JsonMappingFactory;
import com.exasol.adapter.dynamodb.mapping.MappingDefinitionFactory;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchema;
import com.exasol.adapter.dynamodb.queryresultschema.QueryResultTableSchemaBuilder;
import com.exasol.adapter.dynamodb.queryresultschema.RowMapper;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;
import com.exasol.bucketfs.BucketfsFileFactory;
import com.exasol.bucketfs.BucketfsPathException;
import com.exasol.dynamodb.DynamodbConnectionFactory;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalkerException;
import com.exasol.sql.expression.ValueExpression;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter implements VirtualSchemaAdapter {

	@Override
	public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exaMetadata,
			final CreateVirtualSchemaRequest request) throws AdapterException {
		try {
			return runCreateVirtualSchema(request);
		} catch (final IOException exception) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);// NOSONAR
		}
	}

	private CreateVirtualSchemaResponse runCreateVirtualSchema(final CreateVirtualSchemaRequest request)
			throws IOException, AdapterException {
		final SchemaMetadata schemaMetadata = getSchemaMetadata(request);
		return CreateVirtualSchemaResponse.builder().schemaMetadata(schemaMetadata).build();
	}

	private SchemaMappingDefinition getSchemaMappingDefinition(final AdapterRequest request)
			throws AdapterException, IOException {
		final AdapterProperties adapterProperties = new AdapterProperties(
				request.getSchemaMetadataInfo().getProperties());
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		final File mappingDefinitionFile = openSchemaMapping(dynamodbAdapterProperties);
		final MappingDefinitionFactory mappingFactory = new JsonMappingFactory(mappingDefinitionFile);
		return mappingFactory.getSchemaMapping();
	}

	private File openSchemaMapping(final DynamodbAdapterProperties dynamodbAdapterProperties) throws AdapterException {
		try {
			final String path = dynamodbAdapterProperties.getMappingDefinition();
			final File file = new BucketfsFileFactory().openFile(path);
			if (!file.exists()) {
				throw new AdapterException("The specified mapping file (" + file + ") could not be found.");
			}
			return file;
		} catch (final BucketfsPathException exception) {
			throw new AdapterException("Could not open mapping definition", exception);
		}
	}

	private SchemaMetadata getSchemaMetadata(final AdapterRequest request) throws IOException, AdapterException {
		final SchemaMappingDefinition schemaMappingDefinition = getSchemaMappingDefinition(request);
		return SchemaMappingDefinitionToSchemaMetadataConverter.convert(schemaMappingDefinition);
	}

	/**
	 * Creates a connection to DynamoDB using the connection details set in
	 * {@code CREATE CONNECTION}.
	 */
	private AmazonDynamoDB getConnection(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
			throws ExaConnectionAccessException {
		final AdapterProperties properties = getPropertiesFromRequest(request);
		final ExaConnectionInformation connection = exaMetadata.getConnection(properties.getConnectionName());
		return new DynamodbConnectionFactory().getLowLevelConnection(connection.getAddress(), connection.getUser(),
				connection.getPassword());
	}

	private AdapterProperties getPropertiesFromRequest(final AdapterRequest request) {
		return new AdapterProperties(request.getSchemaMetadataInfo().getProperties());
	}

	@Override
	public DropVirtualSchemaResponse dropVirtualSchema(final ExaMetadata exaMetadata,
			final DropVirtualSchemaRequest dropVirtualSchemaRequest) {
		return DropVirtualSchemaResponse.builder().build();
	}

	@Override
	public GetCapabilitiesResponse getCapabilities(final ExaMetadata exaMetadata,
			final GetCapabilitiesRequest getCapabilitiesRequest) {
		final Capabilities.Builder builder = Capabilities.builder();
		final Capabilities capabilities = builder.build();
		return GetCapabilitiesResponse //
				.builder()//
				.capabilities(capabilities)//
				.build();
	}

	/**
	 * Runs the actual query. The data is fetched using a scan from DynamoDB and
	 * then transformed into a {@code SELECT FROM VALUES} statement and passed back
	 * to Exasol.
	 *
	 * @param exaMetadata
	 * @param request
	 * @return
	 * @throws AdapterException
	 */
	@Override
	public PushDownResponse pushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
			throws AdapterException {
		try {
			return runPushdown(exaMetadata, request);
		} catch (final ExaConnectionAccessException | DynamodbResultWalkerException exception) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);// NOSONAR
		}
	}

	private PushDownResponse runPushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
			throws AdapterException, ExaConnectionAccessException {
		final QueryResultTableSchema queryResultTableSchema = new QueryResultTableSchemaBuilder()
				.build(request.getSelect());
		final ScanResult scanResult = runDynamodbQuery(exaMetadata, request);
		final String selectFromValuesStatement = convertResult(scanResult, queryResultTableSchema);
		return PushDownResponse.builder()//
				.pushDownSql(selectFromValuesStatement)//
				.build();
	}

	private String convertResult(final ScanResult scanResult, final QueryResultTableSchema queryResultTableSchema)
			throws AdapterException {
		final List<List<ValueExpression>> resultRows = new ArrayList<>();
		final RowMapper rowMapper = new RowMapper(queryResultTableSchema);
		for (final Map<String, AttributeValue> dynamodbItem : scanResult.getItems()) {
			resultRows.add(rowMapper.mapRow(dynamodbItem));
		}
		return new ValueExpressionsToSqlSelectFromValuesConverter().convert(queryResultTableSchema, resultRows);
	}

	private ScanResult runDynamodbQuery(final ExaMetadata exaMetadata, final PushDownRequest request)
			throws ExaConnectionAccessException {
		final AmazonDynamoDB client = getConnection(exaMetadata, request);
		return client.scan(new ScanRequest("JB_Books"));
	}

	@Override
	public RefreshResponse refresh(final ExaMetadata exaMetadata, final RefreshRequest refreshRequest)
			throws AdapterException {
		try {
			return this.runRefresh(refreshRequest);
		} catch (final IOException exception) {
			throw new AdapterException("Unable update Virtual Schema \"" + refreshRequest.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);// NOSONAR
		}
	}

	private RefreshResponse runRefresh(final RefreshRequest refreshRequest) throws IOException, AdapterException {
		final SchemaMetadata schemaMetadata = getSchemaMetadata(refreshRequest);
		return RefreshResponse.builder().schemaMetadata(schemaMetadata).build();
	}

	@Override
	public SetPropertiesResponse setProperties(final ExaMetadata exaMetadata,
			final SetPropertiesRequest setPropertiesRequest) {
		throw new UnsupportedOperationException("not yet implemented");// NOSONAR (string constant)
	}
}
