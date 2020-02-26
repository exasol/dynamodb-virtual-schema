package com.exasol.adapter.dynamodb;

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
import com.exasol.adapter.response.CreateVirtualSchemaResponse;
import com.exasol.adapter.response.DropVirtualSchemaResponse;
import com.exasol.adapter.response.GetCapabilitiesResponse;
import com.exasol.adapter.response.PushDownResponse;
import com.exasol.adapter.response.RefreshResponse;
import com.exasol.adapter.response.SetPropertiesResponse;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DynamodbAdapter implements VirtualSchemaAdapter{

	private static final Logger LOGGER = Logger.getLogger(DynamodbAdapter.class.getName());

	/**
	 	returnes a hard coded table "testTable" with only one column testCol
	 **/
	@Override
	public CreateVirtualSchemaResponse createVirtualSchema(ExaMetadata exaMetadata, CreateVirtualSchemaRequest request) throws AdapterException {
		List<TableMetadata> tables = new LinkedList<TableMetadata>();
		List<ColumnMetadata> cols = new LinkedList<ColumnMetadata>();
		ColumnMetadata.Builder b = new ColumnMetadata.Builder();
		b.name("isbn");
		b.type(DataType.createVarChar(100, DataType.ExaCharset.ASCII));
		cols.add(b.build());
		tables.add(new TableMetadata("testTable","",cols,""));
		SchemaMetadata remoteMeta = new SchemaMetadata("",tables);
		return CreateVirtualSchemaResponse.builder().schemaMetadata(remoteMeta).build();
	}

	private DynamoDbClient getConnection(ExaMetadata exaMetadata, AbstractAdapterRequest request) throws ExaConnectionAccessException {
		final AdapterProperties properties = getPropertiesFromRequest(request);
		ExaConnectionInformation con = exaMetadata.getConnection(properties.getConnectionName());
		return this.getDynamodbConnection(con.getAddress(),con.getUser(),con.getPassword());
	}

	private AdapterProperties getPropertiesFromRequest(final AdapterRequest request) {
		return new AdapterProperties(request.getSchemaMetadataInfo().getProperties());
	}

	@Override
	public DropVirtualSchemaResponse dropVirtualSchema(ExaMetadata arg0, DropVirtualSchemaRequest arg1)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GetCapabilitiesResponse getCapabilities(ExaMetadata arg0, GetCapabilitiesRequest arg1)
			throws AdapterException {
		final Capabilities.Builder builder = Capabilities.builder();
		Capabilities capabilities = builder.build();
		return GetCapabilitiesResponse //
				.builder()//
				.capabilities(capabilities)//
				.build();
	}

	@Override
	public PushDownResponse pushdown(ExaMetadata exaMetadata, PushDownRequest request) throws AdapterException {
		try{
			DynamoDbClient client = getConnection(exaMetadata,request);
			ScanResponse res = client.scan(ScanRequest.builder().tableName("JB_Books").build());
			StringBuilder respB = new StringBuilder("SELECT * FROM VALUES(");
			boolean first = true;
			for(Map<String, AttributeValue> item : res.items()){
				if(!first) {
					respB.append(", ");
					first = false;
				}
				respB.append(item.get("isbn").s());
			}

			respB.append(");");

			PushDownResponse.Builder builder = new PushDownResponse.Builder();
			builder.pushDownSql(respB.toString());
			return builder.build();
		} catch (ExaConnectionAccessException exception) {
			throw new AdapterException("Unable create Virtual Schema \"" + request.getVirtualSchemaName()
					+ "\". Cause: \"" + exception.getMessage(), exception);
		}
	}

	@Override
	public RefreshResponse refresh(ExaMetadata arg0, RefreshRequest arg1) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SetPropertiesResponse setProperties(ExaMetadata arg0, SetPropertiesRequest arg1) throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}


	protected DynamoDbClient getDynamodbConnection(String uri, String user, String key){
		StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(user, key));
		DynamoDbClientBuilder cilentBuilder = DynamoDbClient.builder().region(Region.EU_CENTRAL_1).credentialsProvider(credentialsProvider);
		if(!uri.equals("aws")){
			cilentBuilder.endpointOverride(URI.create(uri));
		}
		return cilentBuilder.build();
	}



}
