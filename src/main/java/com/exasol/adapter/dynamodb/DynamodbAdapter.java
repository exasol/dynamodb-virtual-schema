package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private CreateVirtualSchemaResponse runCreateVirtualSchema(final CreateVirtualSchemaRequest request)
            throws IOException, AdapterException {
        final SchemaMetadata schemaMetadata = getSchemaMetadata(request);
        return CreateVirtualSchemaResponse.builder().schemaMetadata(schemaMetadata).build();
    }

    private SchemaMetadata getSchemaMetadata(final AdapterRequest request) throws IOException, AdapterException {
        final SchemaMappingDefinition schemaMappingDefinition = getSchemaMappingDefinition(request);
        return new SchemaMappingDefinitionToSchemaMetadataConverter().convert(schemaMappingDefinition);
    }

    private SchemaMappingDefinition getSchemaMappingDefinition(final AdapterRequest request)
            throws AdapterException, IOException {
        final AdapterProperties adapterProperties = new AdapterProperties(
                request.getSchemaMetadataInfo().getProperties());
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        final File mappingDefinitionFile = getSchemaMappingFile(dynamodbAdapterProperties);
        final MappingDefinitionFactory mappingFactory = new JsonMappingFactory(mappingDefinitionFile);
        return mappingFactory.getSchemaMapping();
    }

    private File getSchemaMappingFile(final DynamodbAdapterProperties dynamodbAdapterProperties)
            throws AdapterException {
        final String path = dynamodbAdapterProperties.getMappingDefinition();
        final File file = new BucketfsFileFactory().openFile(path);
        if (!file.exists()) {
            throw new AdapterException("The specified mapping file (" + file
                    + ") could not be found. Make sure you uploaded your mapping definition to BucketFS and specified "
                    + "the correct bucketfs, bucket and path within the bucket.");
        }
        return file;
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
     * Runs the actual query. The data is fetched using a scan from DynamoDB and then transformed into a
     * {@code SELECT FROM VALUES} statement and passed back to Exasol.
     */
    @Override
    public PushDownResponse pushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
            throws AdapterException {
        try {
            return runPushdown(exaMetadata, request);
        } catch (final ExaConnectionAccessException | DynamodbResultWalkerException | IOException exception) {
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private PushDownResponse runPushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
            throws AdapterException, ExaConnectionAccessException, IOException {
        final QueryResultTableSchema queryResultTableSchema = new QueryResultTableSchemaBuilder()
                .build(request.getSelect(), getSchemaMetadata(request));

        final QueryRunner queryRunner = new QueryRunner(getConnectionInformation(exaMetadata, request));
        final RowMapper rowMapper = new RowMapper(queryResultTableSchema);
        final List<List<ValueExpression>> resultRows = new ArrayList<>();
        queryRunner.runQuery(queryResultTableSchema).forEach(dynamodbRow -> {
            resultRows.add(rowMapper.mapRow(dynamodbRow));
        });
        final String selectFromValuesStatement = new ValueExpressionsToSqlSelectFromValuesConverter()
                .convert(queryResultTableSchema, resultRows);

        return PushDownResponse.builder()//
                .pushDownSql(selectFromValuesStatement)//
                .build();
    }

    private ExaConnectionInformation getConnectionInformation(final ExaMetadata exaMetadata,
            final PushDownRequest request) throws ExaConnectionAccessException {
        final AdapterProperties properties = getPropertiesFromRequest(request);
        return exaMetadata.getConnection(properties.getConnectionName());
    }

    @Override
    public RefreshResponse refresh(final ExaMetadata exaMetadata, final RefreshRequest refreshRequest)
            throws AdapterException {
        try {
            return runRefresh(refreshRequest);
        } catch (final IOException exception) {
            throw new AdapterException("Unable to update Virtual Schema \"" + refreshRequest.getVirtualSchemaName()
                    + "\". Cause: " + exception.getMessage(), exception);
        }
    }

    private RefreshResponse runRefresh(final RefreshRequest refreshRequest) throws IOException, AdapterException {
        final SchemaMetadata schemaMetadata = getSchemaMetadata(refreshRequest);
        return RefreshResponse.builder().schemaMetadata(schemaMetadata).build();
    }

    @Override
    public SetPropertiesResponse setProperties(final ExaMetadata exaMetadata,
            final SetPropertiesRequest setPropertiesRequest) {
        throw new UnsupportedOperationException(
                "The current version of DynamoDB Virtual Schema does not support SET PROPERTIES statement.");
    }
}
