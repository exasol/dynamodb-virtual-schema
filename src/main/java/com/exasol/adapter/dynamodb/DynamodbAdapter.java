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
import com.exasol.adapter.dynamodb.mapping.MappingFactory;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.queryresult.QueryResultTable;
import com.exasol.adapter.dynamodb.queryresult.QueryResultTableBuilder;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;
import com.exasol.cellvalue.CellValuesToSqlSelectFromValuesConverter;
import com.exasol.cellvalue.ExasolCellValue;
import com.exasol.dynamodb.DynamodbConnectionUtil;
import com.exasol.dynamodb.resultwalker.DynamodbResultWalker;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter implements VirtualSchemaAdapter {

	@Override
	public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exaMetadata,
			final CreateVirtualSchemaRequest request) throws AdapterException {
		try {
			final SchemaMappingDefinition schemaMappingDefinition = getSchemaMappingDefinition(request);
			final SchemaMetadata schemaMetadata = SchemaMappingDefinitionToSchemaMetadataConverter
					.convert(schemaMappingDefinition);
			return CreateVirtualSchemaResponse.builder().schemaMetadata(schemaMetadata).build();
		} catch (final IOException e) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + e.getMessage(), e);
		}
	}

	private SchemaMappingDefinition getSchemaMappingDefinition(final CreateVirtualSchemaRequest request)
			throws AdapterException, IOException {
		final AdapterProperties adapterProperties = new AdapterProperties(
				request.getSchemaMetadataInfo().getProperties());
		final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
		final File path = dynamodbAdapterProperties.getMappingDefinition();
		if (!path.exists()) {
			throw new AdapterException(String.format("the specified mapping file (%s) could not be found.", path));
		}
		final MappingFactory mappingFactory = new JsonMappingFactory(path);
		return mappingFactory.getSchemaMapping();
	}

	/**
	 * Creates a connection to DynamoDB using the connection details set in
	 * {@code CREATE CONNECTION}.
	 */
	private AmazonDynamoDB getConnection(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
			throws ExaConnectionAccessException {
		final AdapterProperties properties = getPropertiesFromRequest(request);
		final ExaConnectionInformation connection = exaMetadata.getConnection(properties.getConnectionName());
		return DynamodbConnectionUtil.getLowLevelConnection(connection.getAddress(), connection.getUser(),
				connection.getPassword());
	}

	private AdapterProperties getPropertiesFromRequest(final AdapterRequest request) {
		return new AdapterProperties(request.getSchemaMetadataInfo().getProperties());
	}

	@Override
	public DropVirtualSchemaResponse dropVirtualSchema(final ExaMetadata exaMetadata,
			final DropVirtualSchemaRequest dropVirtualSchemaRequest) {
		throw new UnsupportedOperationException("not yet implemented");// NOSONAR (string constant)
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

		final QueryResultTableBuilder queryResultTableBuilder = new QueryResultTableBuilder();
		request.getSelect().accept(queryResultTableBuilder);
		final QueryResultTable queryResultTable = queryResultTableBuilder.getQueryResultTable();
		try {
			final AmazonDynamoDB client = getConnection(exaMetadata, request);
			final ScanResult scanResult = client.scan(new ScanRequest("JB_Books"));
			final List<List<ExasolCellValue>> resultRows = new ArrayList<>();
			for (final Map<String, AttributeValue> dynamodbItem : scanResult.getItems()) {
				resultRows.add(queryResultTable.convertRow(dynamodbItem));
			}
			final String selectFromValuesStatement = new CellValuesToSqlSelectFromValuesConverter()
					.convert(queryResultTable, resultRows);
			return PushDownResponse.builder()//
					.pushDownSql(selectFromValuesStatement)//
					.build();
		} catch (final ExaConnectionAccessException | DynamodbResultWalker.DynamodbResultWalkerException exception) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);
		}
	}

	@Override
	public RefreshResponse refresh(final ExaMetadata exaMetadata, final RefreshRequest refreshRequest) {
		throw new UnsupportedOperationException("not yet implemented");// NOSONAR (string constant)
	}

	@Override
	public SetPropertiesResponse setProperties(final ExaMetadata exaMetadata,
			final SetPropertiesRequest setPropertiesRequest) {
		throw new UnsupportedOperationException("not yet implemented");// NOSONAR (string constant)
	}
}
