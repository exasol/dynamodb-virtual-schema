package com.exasol.adapter.dynamodb;

import static com.exasol.adapter.capabilities.MainCapability.FILTER_EXPRESSIONS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcherFactory;
import com.exasol.adapter.dynamodb.documentfetcher.dynamodb.DynamodbDocumentFetcherFactory;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadataFactoryImplementation;
import com.exasol.adapter.dynamodb.literalconverter.dynamodb.SqlLiteralToDynamodbValueConverter;
import com.exasol.adapter.dynamodb.mapping.*;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbTableKeyFetcher;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQueryFactory;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;
import com.exasol.bucketfs.BucketfsFileFactory;
import com.exasol.dynamodb.DynamodbConnectionFactory;
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
            return runCreateVirtualSchema(exaMetadata, request);
        } catch (final IOException | ExaConnectionAccessException exception) {
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private CreateVirtualSchemaResponse runCreateVirtualSchema(final ExaMetadata exaMetadata,
            final CreateVirtualSchemaRequest request)
            throws IOException, AdapterException, ExaConnectionAccessException {
        final AmazonDynamoDB dynamodbClient = getDynamoDBClient(exaMetadata, request);
        final SchemaMetadata schemaMetadata = createSchemaMetadata(request, dynamodbClient);
        return CreateVirtualSchemaResponse.builder().schemaMetadata(schemaMetadata).build();
    }

    private SchemaMetadata createSchemaMetadata(final AdapterRequest request, final AmazonDynamoDB dynamodbClient)
            throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getSchemaMappingDefinition(request, dynamodbClient);
        return new SchemaMappingToSchemaMetadataConverter().convert(schemaMapping);
    }

    private SchemaMapping getSchemaMappingDefinition(final AdapterRequest request, final AmazonDynamoDB dynamodbClient)
            throws AdapterException, IOException {
        final AdapterProperties adapterProperties = new AdapterProperties(
                request.getSchemaMetadataInfo().getProperties());
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        final File mappingDefinitionFile = getSchemaMappingFile(dynamodbAdapterProperties);
        final DynamodbTableKeyFetcher dynamodbTableKeyFetcher = new DynamodbTableKeyFetcher(
                new DynamodbTableMetadataFactoryImplementation(dynamodbClient));
        final SchemaMappingReader mappingFactory = new JsonSchemaMappingReader(mappingDefinitionFile,
                dynamodbTableKeyFetcher);
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
        } catch (final ExaConnectionAccessException exception) {
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private PushDownResponse runPushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
            throws AdapterException, ExaConnectionAccessException {
        final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery = new RemoteTableQueryFactory<>(
                new SqlLiteralToDynamodbValueConverter()).build(request.getSelect(),
                        request.getSchemaMetadataInfo().getAdapterNotes());
        final String selectFromValuesStatement = runQuery(exaMetadata, request, remoteTableQuery);
        return PushDownResponse.builder()//
                .pushDownSql(selectFromValuesStatement)//
                .build();
    }

    private String runQuery(final ExaMetadata exaMetadata, final PushDownRequest request,
            final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery) throws ExaConnectionAccessException {
        final AmazonDynamoDB dynamodbClient = getDynamoDBClient(exaMetadata, request);
        final DocumentFetcherFactory<DynamodbNodeVisitor> documentFetcherFactory = new DynamodbDocumentFetcherFactory(
                dynamodbClient);
        final SchemaMapper<DynamodbNodeVisitor> schemaMapper = new SchemaMapper<>(remoteTableQuery,
                new DynamodbPropertyToColumnValueExtractorFactory());
        final List<List<ValueExpression>> resultRows = new ArrayList<>();
        final ExaConnectionInformation connectionInformation = getConnectionInformation(exaMetadata, request);
        documentFetcherFactory.buildDocumentFetcherForQuery(remoteTableQuery).run(connectionInformation)
                .forEach(dynamodbRow -> schemaMapper.mapRow(dynamodbRow).forEach(resultRows::add));
        return new ValueExpressionsToSqlSelectFromValuesConverter().convert(remoteTableQuery, resultRows);
    }

    private AmazonDynamoDB getDynamoDBClient(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
            throws ExaConnectionAccessException {
        return new DynamodbConnectionFactory().getLowLevelConnection(getConnectionInformation(exaMetadata, request));
    }

    private ExaConnectionInformation getConnectionInformation(final ExaMetadata exaMetadata,
            final AbstractAdapterRequest request) throws ExaConnectionAccessException {
        final AdapterProperties properties = getPropertiesFromRequest(request);
        return exaMetadata.getConnection(properties.getConnectionName());
    }

    @Override
    public RefreshResponse refresh(final ExaMetadata exaMetadata, final RefreshRequest refreshRequest)
            throws AdapterException {
        try {
            return runRefresh(exaMetadata, refreshRequest);
        } catch (final IOException | ExaConnectionAccessException exception) {
            throw new AdapterException("Unable to update Virtual Schema \"" + refreshRequest.getVirtualSchemaName()
                    + "\". Cause: " + exception.getMessage(), exception);
        }
    }

    private RefreshResponse runRefresh(final ExaMetadata exaMetadata, final RefreshRequest refreshRequest)
            throws IOException, AdapterException, ExaConnectionAccessException {
        final AmazonDynamoDB dynamodbClient = new DynamodbConnectionFactory()
                .getLowLevelConnection(getConnectionInformation(exaMetadata, refreshRequest));
        final SchemaMetadata schemaMetadata = createSchemaMetadata(refreshRequest, dynamodbClient);
        return RefreshResponse.builder().schemaMetadata(schemaMetadata).build();
    }

    @Override
    public SetPropertiesResponse setProperties(final ExaMetadata exaMetadata,
            final SetPropertiesRequest setPropertiesRequest) {
        throw new UnsupportedOperationException(
                "The current version of DynamoDB Virtual Schema does not support SET PROPERTIES statement.");
    }
}
