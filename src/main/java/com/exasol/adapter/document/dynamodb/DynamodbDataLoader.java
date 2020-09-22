package com.exasol.adapter.document.dynamodb;

import com.exasol.adapter.document.AbstractDataLoader;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.document.mapping.PropertyToColumnValueExtractorFactory;
import com.exasol.adapter.document.mapping.dynamodb.DynamodbPropertyToColumnValueExtractorFactory;

public class DynamodbDataLoader extends AbstractDataLoader<DynamodbNodeVisitor> {

    private static final long serialVersionUID = -6648852900087989808L;

    /**
     * Create a new instance of {@link DynamodbDataLoader}.
     *
     * @param documentFetcher document fetcher that provides the document data.
     */
    public DynamodbDataLoader(final DocumentFetcher<DynamodbNodeVisitor> documentFetcher) {
        super(documentFetcher);
    }

    @Override
    protected PropertyToColumnValueExtractorFactory<DynamodbNodeVisitor> getValueExtractorFactory() {
        return new DynamodbPropertyToColumnValueExtractorFactory();
    }
}
