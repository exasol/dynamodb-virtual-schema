package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.*;
import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnValueExtractorFactory;
import com.exasol.adapter.dynamodb.mapping.SchemaMapper;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.sql.expresion.ValueExpressionToJavaObjectConverter;
import com.exasol.utils.StringSerializer;

/**
 * This class is the abstract basis fot the database specific UDF call. In the UDF call the document data is fetched by
 * the {@link DocumentFetcher}, mapped by the {@link SchemaMapper} and finally emitted to the Exasol database.
 * 
 * To save memory and process huge amounts of data, this process is implemented as a pipeline. That means that fetching
 * mapping and emitting of the rows is done for each row and not en-block.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
abstract class AbstractUdf<DocumentVisitorType> {
    public static final String PARAMETER_DOCUMENT_FETCHER = "DOCUMENT_FETCHER";
    public static final String PARAMETER_REMOTE_TABLE_QUERY = "REMOTE_TABLE_QUERY";
    public static final String PARAMETER_CONNECTION_NAME = "CONNECTION_NAME";

    public final void run(final ExaMetadata exaMetadata, final ExaIterator exaIterator) throws ClassNotFoundException,
            ExaIterationException, ExaDataTypeException, IOException, ExaConnectionAccessException {
        // TODO handle multiple rows
        final RemoteTableQuery<DocumentVisitorType> remoteTableQuery = deserializeRemoteTableQuery(exaIterator);
        final SchemaMapper<DocumentVisitorType> schemaMapper = new SchemaMapper<>(remoteTableQuery,
                getDocumentVisitorTypePropertyToColumnValueExtractorFactory());

        final DocumentFetcher<DocumentVisitorType> documentFetcher = deserializeDocumentFetcher(exaIterator);
        final ExaConnectionInformation connectionInformation = exaMetadata
                .getConnection(exaIterator.getString(PARAMETER_CONNECTION_NAME));
        final ValueExpressionToJavaObjectConverter valueExpressionToJavaObjectConverter = new ValueExpressionToJavaObjectConverter();
        documentFetcher.run(connectionInformation)
                .forEach(dynamodbRow -> schemaMapper.mapRow(dynamodbRow).map(row -> row.stream()
                        .map(valueExpressionToJavaObjectConverter::convert).collect(Collectors.toList()))
                        .forEach(values -> emitRow(values, exaIterator)));
    }

    protected abstract PropertyToColumnValueExtractorFactory<DocumentVisitorType> getDocumentVisitorTypePropertyToColumnValueExtractorFactory();

    private DocumentFetcher<DocumentVisitorType> deserializeDocumentFetcher(final ExaIterator exaIterator)
            throws ExaIterationException, ExaDataTypeException, IOException, ClassNotFoundException {
        final String serialized = exaIterator.getString(PARAMETER_DOCUMENT_FETCHER);
        return (DocumentFetcher<DocumentVisitorType>) StringSerializer.deserializeFromString(serialized);
    }

    private RemoteTableQuery<DocumentVisitorType> deserializeRemoteTableQuery(final ExaIterator exaIterator)
            throws ExaIterationException, ExaDataTypeException, IOException, ClassNotFoundException {
        final String serialized = exaIterator.getString(PARAMETER_REMOTE_TABLE_QUERY);
        return (RemoteTableQuery<DocumentVisitorType>) StringSerializer.deserializeFromString(serialized);
    }

    private void emitRow(final List<Object> row, final ExaIterator iterator) {
        try {
            iterator.emit(row.toArray());
        } catch (final ExaIterationException | ExaDataTypeException e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
