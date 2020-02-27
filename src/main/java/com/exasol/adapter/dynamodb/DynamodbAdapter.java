package com.exasol.adapter.dynamodb;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class DynamodbAdapter implements VirtualSchemaAdapter {

	private static final Logger LOGGER = Logger.getLogger(DynamodbAdapter.class.getName());

	/**
	 * creates a hard coded table "testTable" with only one column testCol
	 **/
	@Override
	public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exaMetadata,
			final CreateVirtualSchemaRequest request) throws AdapterException {
		final List<TableMetadata> tables = new LinkedList<>();
		final ColumnMetadata.Builder colBuilder = new ColumnMetadata.Builder();
		colBuilder.name("isbn");
		colBuilder.type(DataType.createVarChar(100, DataType.ExaCharset.ASCII));
		final List<ColumnMetadata> cols = new LinkedList<>();
		cols.add(colBuilder.build());
		tables.add(new TableMetadata("testTable", "", cols, ""));
		final SchemaMetadata remoteMeta = new SchemaMetadata("", tables);
		return CreateVirtualSchemaResponse.builder().schemaMetadata(remoteMeta).build();
	}

	private DynamoDbClient getConnection(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
			throws ExaConnectionAccessException {
		final AdapterProperties properties = getPropertiesFromRequest(request);
		final ExaConnectionInformation connection = exaMetadata.getConnection(properties.getConnectionName());
		return this.getDynamodbConnection(connection.getAddress(), connection.getUser(), connection.getPassword());
	}

	private AdapterProperties getPropertiesFromRequest(final AdapterRequest request) {
		return new AdapterProperties(request.getSchemaMetadataInfo().getProperties());
	}

	@Override
	public DropVirtualSchemaResponse dropVirtualSchema(final ExaMetadata arg0, final DropVirtualSchemaRequest arg1)
			throws AdapterException {
		return null;
	}

	@Override
	public GetCapabilitiesResponse getCapabilities(final ExaMetadata arg0, final GetCapabilitiesRequest arg1)
			throws AdapterException {
		final Capabilities.Builder builder = Capabilities.builder();
		final Capabilities capabilities = builder.build();
		return GetCapabilitiesResponse //
				.builder()//
				.capabilities(capabilities)//
				.build();
	}

	@Override
	public PushDownResponse pushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
			throws AdapterException {
		try {
			final DynamoDbClient client = getConnection(exaMetadata, request);
			final ScanResponse res = client.scan(ScanRequest.builder().tableName("JB_Books").build());
			final StringBuilder responseBuilder = new StringBuilder("SELECT * FROM (VALUES");
			boolean isFirst = true;
			for (final Map<String, AttributeValue> item : res.items()) {
				if (!isFirst) {
					responseBuilder.append(", ");
				}
				isFirst = false;
				responseBuilder.append("(").append(item.get("isbn").s()).append(")");
			}

			responseBuilder.append(");");

			final PushDownResponse.Builder builder = new PushDownResponse.Builder();
			builder.pushDownSql(responseBuilder.toString());
			return builder.build();
		} catch (final ExaConnectionAccessException exception) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);
		}
	}

	@Override
	public RefreshResponse refresh(final ExaMetadata arg0, final RefreshRequest arg1) throws AdapterException {
		return null;
	}

	@Override
	public SetPropertiesResponse setProperties(final ExaMetadata arg0, final SetPropertiesRequest arg1)
			throws AdapterException {
		return null;
	}

	protected DynamoDbClient getDynamodbConnection(final String uri, final String user, final String key) {
		final StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider
				.create(AwsBasicCredentials.create(user, key));
		final DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder().region(Region.EU_CENTRAL_1)
				.credentialsProvider(credentialsProvider);
		if (!uri.equals("aws")) {
			clientBuilder.endpointOverride(URI.create(uri));
		}
		return clientBuilder.build();
	}
}
