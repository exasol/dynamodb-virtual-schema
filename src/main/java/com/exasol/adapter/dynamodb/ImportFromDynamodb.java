package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.*;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.mapping.SchemaMapper;
import com.exasol.adapter.dynamodb.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.sql.expresion.ValueExpressionToJavaObjectConverter;
import com.exasol.utils.StringSerializer;

/**
 * Main UDF entry point. Per convention, the UDF Script must have the same name as the main class.
 */
public class ImportFromDynamodb {
    // TODO split general and DynamoDB part
    public static final String PARAMETER_DOCUMENT_FETCHER = "DOCUMENT_FETCHER";
    public static final String PARAMETER_REMOTE_TABLE_QUERY = "REMOTE_TABLE_QUERY";
    public static final String PARAMETER_CONNECTION_NAME = "CONNECTION_NAME";

    /**
     * This method is called by the Exasol database when the ImportFromDynamodb UDF is called.
     * 
     * @param exaMetadata exasol metadata
     * @param exaIterator iterator
     * @throws Exception if data can't get loaded
     */
    public static void run(final ExaMetadata exaMetadata, final ExaIterator exaIterator) throws Exception {
        // TODO handle multiple rows
        final DocumentFetcher<DynamodbNodeVisitor> documentFetcher = deserializeDocumentFetcher(exaIterator);
        final RemoteTableQuery<DynamodbNodeVisitor> remoteTableQuery = deserializeRemoteTableQuery(exaIterator);
        final SchemaMapper<DynamodbNodeVisitor> schemaMapper = new SchemaMapper<>(remoteTableQuery,
                new DynamodbPropertyToColumnValueExtractorFactory());
        final ExaConnectionInformation connectionInformation = exaMetadata
                .getConnection(exaIterator.getString(PARAMETER_CONNECTION_NAME));
        final ValueExpressionToJavaObjectConverter valueExpressionToJavaObjectConverter = new ValueExpressionToJavaObjectConverter();
        documentFetcher.run(connectionInformation).forEach(dynamodbRow -> {
            schemaMapper.mapRow(dynamodbRow).map(
                    row -> row.stream().map(valueExpressionToJavaObjectConverter::convert).collect(Collectors.toList()))
                    .forEach(values -> emitRow(values, exaIterator));
        });
    }

    private static void emitRow(final List<Object> row, final ExaIterator iterator) {
        try {
            iterator.emit(row.toArray());
        } catch (final ExaIterationException | ExaDataTypeException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    private static DocumentFetcher<DynamodbNodeVisitor> deserializeDocumentFetcher(final ExaIterator exaIterator)
            throws ExaIterationException, ExaDataTypeException, IOException, ClassNotFoundException {
        final String serialized = exaIterator.getString(PARAMETER_DOCUMENT_FETCHER);
        return (DocumentFetcher<DynamodbNodeVisitor>) StringSerializer.deserializeFromString(serialized);
    }

    private static RemoteTableQuery<DynamodbNodeVisitor> deserializeRemoteTableQuery(final ExaIterator exaIterator)
            throws ExaIterationException, ExaDataTypeException, IOException, ClassNotFoundException {
        final String serialized = exaIterator.getString(PARAMETER_REMOTE_TABLE_QUERY);
        return (RemoteTableQuery<DynamodbNodeVisitor>) StringSerializer.deserializeFromString(serialized);
    }
}
