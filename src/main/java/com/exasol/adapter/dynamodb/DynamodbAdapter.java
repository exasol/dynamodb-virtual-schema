package com.exasol.adapter.dynamodb;

import java.net.URI;
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

/**
 * DynamoDB Virtual Schema adapter
 */
public class DynamodbAdapter implements VirtualSchemaAdapter {

	private static final Logger LOGGER = Logger.getLogger(DynamodbAdapter.class.getName());

	/**
	 * Creates a hard coded table {@code testTable} with only one column {@code testCol}
	 **/
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
	 * Creates a connection to DynamoDB using the connection details set in {@code CREATE CONNECTION}
	 * 
	 * @param exaMetadata
	 * @param request
	 * @return DynamoDB client
	 */
	private DynamoDbClient getConnection(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
			throws ExaConnectionAccessException {
		final AdapterProperties properties = getPropertiesFromRequest(request);
		final ExaConnectionInformation connection = exaMetadata.getConnection(properties.getConnectionName());
		return getDynamodbConnection(connection.getAddress(), connection.getUser(), connection.getPassword());
	}

	/**
	 * Creates a DynamoDB client for a given uri, user and key
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
	protected static DynamoDbClient getDynamodbConnection(final String uri, final String user, final String key) {
		final StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider
				.create(AwsBasicCredentials.create(user, key));
		final DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder().credentialsProvider(credentialsProvider);
		final String AWS_PREFIX = "aws:";
		if (uri.startsWith(AWS_PREFIX)) {
			clientBuilder.region(Region.of(uri.replace(AWS_PREFIX, "")));
		} else {
			clientBuilder.region(Region.EU_CENTRAL_1);
			clientBuilder.endpointOverride(URI.create(uri));
		}
		return clientBuilder.build();
	}

	private AdapterProperties getPropertiesFromRequest(final AdapterRequest request) {
		return new AdapterProperties(request.getSchemaMetadataInfo().getProperties());
	}

	@Override
	public DropVirtualSchemaResponse dropVirtualSchema(final ExaMetadata arg0, final DropVirtualSchemaRequest arg1) {
		return null;
	}

	@Override
	public GetCapabilitiesResponse getCapabilities(final ExaMetadata arg0, final GetCapabilitiesRequest arg1) {
		final Capabilities.Builder builder = Capabilities.builder();
		final Capabilities capabilities = builder.build();
		return GetCapabilitiesResponse //
				.builder()//
				.capabilities(capabilities)//
				.build();
	}

	/**
	 * Runs the actual query. The data is fetched using a scan from DynamoDB and
	 * then transformed into a {@code SELECT FROM VALUES} statement and passed back to
	 * Exasol
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
			final DynamoDbClient client = getConnection(exaMetadata, request);
			final ScanResponse scanResponse = client.scan(ScanRequest.builder().tableName("JB_Books").build());
			final String selectFromValuesStatement = dynamodbResultToSelectFromValues(scanResponse);
			final PushDownResponse.Builder responseBuilder = new PushDownResponse.Builder();
			responseBuilder.pushDownSql(selectFromValuesStatement);
			return responseBuilder.build();
		} catch (final ExaConnectionAccessException exception) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);
		}
	}

	private String dynamodbResultToSelectFromValues(final ScanResponse scanResponse) {
		final List<Map<String, AttributeValue>> scannedItems = scanResponse.items();
		if (scannedItems.size() == 0) {
			return "SELECT * FROM VALUES('') WHERE 0 = 1;";
		}
		final StringBuilder responseBuilder = new StringBuilder("SELECT * FROM (VALUES");
		boolean isFirst = true;
		for (final Map<String, AttributeValue> item : scannedItems) {
			if (!isFirst) {
				responseBuilder.append(", ");
			}
			isFirst = false;
			responseBuilder.append("('").append(item.get("isbn").s()).append("')");
		}
		responseBuilder.append(");");
		return responseBuilder.toString();
	}

	@Override
	public RefreshResponse refresh(final ExaMetadata arg0, final RefreshRequest arg1) {
		return null;
	}

	@Override
	public SetPropertiesResponse setProperties(final ExaMetadata arg0, final SetPropertiesRequest arg1) {
		return null;
	}
}
