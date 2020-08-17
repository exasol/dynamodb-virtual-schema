package com.exasol.adapter.document;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.*;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.mapping.PropertyToColumnValueExtractorFactory;
import com.exasol.adapter.document.mapping.SchemaMapper;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.sql.expresion.ValueExpressionToJavaObjectConverter;
import com.exasol.utils.StringSerializer;

/**
 * This class is the abstract basis for the database-specific UDF call. In the UDF call, the document data is fetched by
 * the {@link DocumentFetcher}, mapped by the {@link SchemaMapper} and finally emitted to the Exasol database.
 * 
 * To save memory and process huge amounts of data, this process is implemented as a pipeline. That means that fetching,
 * mapping and emitting of the rows is done for each row and not en-block.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class AbstractDataLoaderUdf<DocumentVisitorType> implements DataLoaderUdf {
    public static final String PARAMETER_DOCUMENT_FETCHER = "DOCUMENT_FETCHER";
    public static final String PARAMETER_REMOTE_TABLE_QUERY = "REMOTE_TABLE_QUERY";
    public static final String PARAMETER_CONNECTION_NAME = "CONNECTION_NAME";

    @Override
    public final void run(final ExaMetadata exaMetadata, final ExaIterator exaIterator) throws ClassNotFoundException,
            ExaIterationException, ExaDataTypeException, IOException, ExaConnectionAccessException {
        final RemoteTableQuery remoteTableQuery = deserializeRemoteTableQuery(exaIterator);
        final SchemaMapper<DocumentVisitorType> schemaMapper = new SchemaMapper<>(remoteTableQuery,
                getValueExtractorFactory());
        do {
            runSingleDocumentFetcher(exaMetadata, exaIterator, schemaMapper);
        } while (exaIterator.next());
    }

    private void runSingleDocumentFetcher(final ExaMetadata exaMetadata, final ExaIterator exaIterator,
            final SchemaMapper<DocumentVisitorType> schemaMapper) throws ExaIterationException, ExaDataTypeException,
            IOException, ClassNotFoundException, ExaConnectionAccessException {
        final DocumentFetcher<DocumentVisitorType> documentFetcher = deserializeDocumentFetcher(exaIterator);
        final ExaConnectionInformation connectionInformation = exaMetadata
                .getConnection(exaIterator.getString(PARAMETER_CONNECTION_NAME));
        final ValueExpressionToJavaObjectConverter valueExpressionToJavaObjectConverter = new ValueExpressionToJavaObjectConverter();
        documentFetcher.run(connectionInformation)
                .forEach(dynamodbRow -> schemaMapper.mapRow(dynamodbRow).map(row -> row.stream()
                        .map(valueExpressionToJavaObjectConverter::convert).collect(Collectors.toList()))
                        .forEach(values -> emitRow(values, exaIterator)));
    }

    /**
     * Get a database specific {@link PropertyToColumnValueExtractorFactory}.
     * 
     * @return database specific {@link PropertyToColumnValueExtractorFactory}
     */
    protected abstract PropertyToColumnValueExtractorFactory<DocumentVisitorType> getValueExtractorFactory();

    private DocumentFetcher<DocumentVisitorType> deserializeDocumentFetcher(final ExaIterator exaIterator)
            throws ExaIterationException, ExaDataTypeException, IOException, ClassNotFoundException {
        final String serialized = exaIterator.getString(PARAMETER_DOCUMENT_FETCHER);
        return (DocumentFetcher<DocumentVisitorType>) StringSerializer.deserializeFromString(serialized);
    }

    private RemoteTableQuery deserializeRemoteTableQuery(final ExaIterator exaIterator)
            throws ExaIterationException, ExaDataTypeException, IOException, ClassNotFoundException {
        final String serialized = exaIterator.getString(PARAMETER_REMOTE_TABLE_QUERY);
        return (RemoteTableQuery) StringSerializer.deserializeFromString(serialized);
    }

    private void emitRow(final List<Object> row, final ExaIterator iterator) {
        try {
            iterator.emit(row.toArray());
        } catch (final ExaIterationException | ExaDataTypeException e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
