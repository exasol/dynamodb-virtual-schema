package com.exasol.adapter.dynamodb;

import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter implements VirtualSchemaAdapter {

	@Override
	public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exaMetadata,
			final CreateVirtualSchemaRequest request) {
		final ColumnMetadata.Builder colBuilder = new ColumnMetadata.Builder();
		colBuilder.name("isbn");
		colBuilder.type(DataType.createVarChar(100, DataType.ExaCharset.ASCII));
		final List<ColumnMetadata> cols = List.of(colBuilder.build());
		final List<TableMetadata> tables = List.of(new TableMetadata("testTable", "", cols, ""));
		final SchemaMetadata remoteMeta = new SchemaMetadata("", tables);
		return CreateVirtualSchemaResponse.builder().schemaMetadata(remoteMeta).build();
	}

	/**
	 * Creates a connection to DynamoDB using the connection details set in
	 * {@code CREATE CONNECTION}.
	 */
	private DynamoDB getConnection(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
			throws ExaConnectionAccessException {
		final AdapterProperties properties = getPropertiesFromRequest(request);
		final ExaConnectionInformation connection = exaMetadata.getConnection(properties.getConnectionName());
		return getDynamodbConnection(connection.getAddress(), connection.getUser(), connection.getPassword());
	}

	/**
	 * Creates a DynamoDB client for a given uri, user and key.
	 * 
	 * @param uri
	 *            either aws:<REGION> or address of local DynamoDB server (e.g.
	 *            http://localhost:8000)
	 * @param user
	 *            aws credential id
	 * @param key
	 *            aws credential key
	 * @return DynamoDB client
	 */
	protected static DynamoDB getDynamodbConnection(final String uri, final String user, final String key) {
		final String AWS_PREFIX = "aws:";
		final String AWS_LOCAL_REGION = "eu-central-1";
		final BasicAWSCredentials awsCredentials = new BasicAWSCredentials(user, key);
		final AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
		if (uri.startsWith(AWS_PREFIX)) {
			clientBuilder.withRegion(uri.replace(AWS_PREFIX, ""));
		} else {
			clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(uri, AWS_LOCAL_REGION));
		}
		return new DynamoDB(clientBuilder.build());
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
		try {
			final DynamoDB client = getConnection(exaMetadata, request);
			final Table table = client.getTable("JB_Books");
			final ItemCollection<ScanOutcome> scanResult = table.scan();
			final String selectFromValuesStatement = new DynamodbResultToSqlSelectFromValuesConverter()
					.convert(scanResult);
			return PushDownResponse.builder()//
					.pushDownSql(selectFromValuesStatement)//
					.build();
		} catch (final ExaConnectionAccessException exception) {
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
