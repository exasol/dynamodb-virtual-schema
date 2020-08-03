package com.exasol.adapter.dynamodb;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentfetcher.dynamodb.DynamodbDocumentFetcherFactory;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.dynamodbmetadata.BaseDynamodbTableMetadataFactory;
import com.exasol.adapter.dynamodb.mapping.JsonSchemaMappingReader;
import com.exasol.adapter.dynamodb.mapping.SchemaMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingReader;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbTableKeyFetcher;
import com.exasol.adapter.dynamodb.queryplanning.RemoteTableQuery;
import com.exasol.adapter.dynamodb.queryplanning.RemoteTableQueryFactory;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.response.*;
import com.exasol.bucketfs.BucketfsFileFactory;
import com.exasol.dynamodb.DynamodbConnectionFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB Virtual Schema adapter.
 */
public class DynamodbAdapter implements VirtualSchemaAdapter {
    private static final Capabilities CAPABILITIES = Capabilities.builder()
            .addMain(MainCapability.FILTER_EXPRESSIONS, MainCapability.SELECTLIST_PROJECTION)
            .addPredicate(PredicateCapability.EQUAL, PredicateCapability.LESS, PredicateCapability.LESSEQUAL)
            .addLiteral(LiteralCapability.STRING, LiteralCapability.NULL, LiteralCapability.BOOL,
                    LiteralCapability.DOUBLE, LiteralCapability.EXACTNUMERIC)
            .build();

    @Override
    public CreateVirtualSchemaResponse createVirtualSchema(final ExaMetadata exaMetadata,
            final CreateVirtualSchemaRequest request) throws AdapterException {
        try {
            return runCreateVirtualSchema(exaMetadata, request);
        } catch (final IOException | ExaConnectionAccessException | URISyntaxException exception) {
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private CreateVirtualSchemaResponse runCreateVirtualSchema(final ExaMetadata exaMetadata,
            final CreateVirtualSchemaRequest request)
            throws IOException, AdapterException, ExaConnectionAccessException, URISyntaxException {
        final DynamoDbClient dynamodbClient = getDynamoDBClient(exaMetadata, request);
        final SchemaMetadata schemaMetadata = createSchemaMetadata(request, dynamodbClient);
        return CreateVirtualSchemaResponse.builder().schemaMetadata(schemaMetadata).build();
    }

    private SchemaMetadata createSchemaMetadata(final AdapterRequest request, final DynamoDbClient dynamodbClient)
            throws IOException, AdapterException {
        final SchemaMapping schemaMapping = getSchemaMappingDefinition(request, dynamodbClient);
        return new SchemaMappingToSchemaMetadataConverter().convert(schemaMapping);
    }

    private SchemaMapping getSchemaMappingDefinition(final AdapterRequest request, final DynamoDbClient dynamodbClient)
            throws AdapterException, IOException {
        final AdapterProperties adapterProperties = new AdapterProperties(
                request.getSchemaMetadataInfo().getProperties());
        final DynamodbAdapterProperties dynamodbAdapterProperties = new DynamodbAdapterProperties(adapterProperties);
        final File mappingDefinitionFile = getSchemaMappingFile(dynamodbAdapterProperties);
        final DynamodbTableKeyFetcher dynamodbTableKeyFetcher = new DynamodbTableKeyFetcher(
                new BaseDynamodbTableMetadataFactory(dynamodbClient));
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
        } catch (final ExaConnectionAccessException | IOException | URISyntaxException exception) {
            throw new AdapterException("Unable to create Virtual Schema \"" + request.getVirtualSchemaName() + "\". "
                    + "Cause: " + exception.getMessage(), exception);
        }
    }

    private PushDownResponse runPushdown(final ExaMetadata exaMetadata, final PushDownRequest request)
            throws AdapterException, ExaConnectionAccessException, IOException, URISyntaxException {
        final RemoteTableQuery remoteTableQuery = new RemoteTableQueryFactory().build(request.getSelect(),
                request.getSchemaMetadataInfo().getAdapterNotes());
        final String responseStatement = runQuery(exaMetadata, request, remoteTableQuery);
        return PushDownResponse.builder()//
                .pushDownSql(responseStatement)//
                .build();
    }

    private String runQuery(final ExaMetadata exaMetadata, final PushDownRequest request,
            final RemoteTableQuery remoteTableQuery)
            throws ExaConnectionAccessException, IOException, URISyntaxException {
        final DynamoDbClient dynamodbClient = getDynamoDBClient(exaMetadata, request);
        final DynamodbDocumentFetcherFactory documentFetcherFactory = new DynamodbDocumentFetcherFactory(
                dynamodbClient);

        final int availableClusterCores = getAvailableClusterCores(exaMetadata);
        final List<DocumentFetcher<DynamodbNodeVisitor>> documentFetchers = documentFetcherFactory
                .buildDocumentFetcherForQuery(remoteTableQuery, availableClusterCores);
        final String connectionName = getPropertiesFromRequest(request).getConnectionName();
        return new UdfCallBuilder<DynamodbNodeVisitor>().getUdfCallSql(documentFetchers, remoteTableQuery,
                connectionName);
    }

    /**
     * Get the total number of cores that are available in the cluster. This method assumes that all cluster nodes have
     * an equal number of cores.
     *
     * @param exaMetadata {@link ExaMetadata}
     * @return total number of cores that are available in the cluster
     */
    private int getAvailableClusterCores(final ExaMetadata exaMetadata) {
        final int cores = Runtime.getRuntime().availableProcessors();
        return (int) (exaMetadata.getNodeCount() * cores);
    }

    private DynamoDbClient getDynamoDBClient(final ExaMetadata exaMetadata, final AbstractAdapterRequest request)
            throws ExaConnectionAccessException, URISyntaxException {
        return new DynamodbConnectionFactory().getConnection(getConnectionInformation(exaMetadata, request));
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
        } catch (final IOException | ExaConnectionAccessException | URISyntaxException exception) {
            throw new AdapterException("Unable to update Virtual Schema \"" + refreshRequest.getVirtualSchemaName()
                    + "\". Cause: " + exception.getMessage(), exception);
        }
    }

    private RefreshResponse runRefresh(final ExaMetadata exaMetadata, final RefreshRequest refreshRequest)
            throws IOException, AdapterException, ExaConnectionAccessException, URISyntaxException {
        final DynamoDbClient dynamodbClient = new DynamodbConnectionFactory()
                .getConnection(getConnectionInformation(exaMetadata, refreshRequest));
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
