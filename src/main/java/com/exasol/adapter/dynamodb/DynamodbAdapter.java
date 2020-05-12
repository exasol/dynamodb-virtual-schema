package com.exasol.adapter.dynamodb;

import static com.exasol.adapter.capabilities.MainCapability.FILTER_EXPRESSIONS;

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
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentfetcher.dynamodb.DynamodbDocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.literalconverter.dynamodb.SqlLiteralToDynamodbValueConverter;
import com.exasol.adapter.dynamodb.mapping.*;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbValueMapperFactory;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQueryFactory;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;
import com.exasol.bucketfs.BucketfsFileFactory;
import com.exasol.sql.expression.ValueExpression;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter implements VirtualSchemaAdapter {
    private static final Capabilities CAPABILITIES = Capabilities.builder().addMain(FILTER_EXPRESSIONS)
            .addPredicate(PredicateCapability.EQUAL, PredicateCapability.LESS, PredicateCapability.LESSEQUAL)
            .addLiteral(LiteralCapability.STRING, LiteralCapability.NULL, LiteralCapability.BOOL,
                    LiteralCapability.DOUBLE, LiteralCapability.EXACTNUMERIC)
            .build();

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
        return GetCapabilitiesResponse //
                .builder()//
                .capabilities(CAPABILITIES)//
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
        } catch (final ExaConnectionAccessException | IOException exception) {
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private PushDownResponse runPushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
            throws AdapterException, ExaConnectionAccessException, IOException {
        final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery = new RemoteTableQueryFactory<>(
                new SqlLiteralToDynamodbValueConverter()).build(request.getSelect(), getSchemaMetadata(request));
        final String selectFromValuesStatement = runQuery(exaMetadata, request, remoteTableQuery);
        return PushDownResponse.builder()//
                .pushDownSql(selectFromValuesStatement)//
                .build();
    }

    private String runQuery(final ExaMetadata exaMetadata, final PushDownRequest request,
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery) throws ExaConnectionAccessException {
        final DocumentFetcher documentFetcher = new DynamodbDocumentFetcher(
                getConnectionInformation(exaMetadata, request));
        final SchemaMapper<DynamodbNodeVisitor> schemaMapper = new SchemaMapper<>(remoteTableQuery,
                new DynamodbValueMapperFactory());
        final List<List<ValueExpression>> resultRows = new ArrayList<>();
        documentFetcher.fetchDocumentData(remoteTableQuery)
                .forEach(dynamodbRow -> schemaMapper.mapRow(dynamodbRow).forEach(resultRows::add));
        return new ValueExpressionsToSqlSelectFromValuesConverter().convert(remoteTableQuery, resultRows);
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
