package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.*;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.connection.ConnectionPropertiesReader;
import com.exasol.adapter.document.documentfetcher.FetchedDocument;
import com.exasol.adapter.document.documentnode.DocumentObject;
import com.exasol.adapter.document.documentnode.DocumentStringValue;
import com.exasol.adapter.document.iterators.CloseableIterator;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class AbstractDynamodbDocumentFetcherTest {
    private static final String MY_TABLE_NAME = "myTableName";
    private static final String KEY = "myKey";
    private static final String VALUE = "myStringValue";

    @Test
    void testRun() {
        final List<FetchedDocument> results = new ArrayList<>();
        try (final CloseableIterator<FetchedDocument> iterator = new Stub().run(new ConnectionPropertiesReader(
                "{ \"awsAccessKeyId\": \"abc\", \"awsSecretAccessKey\": \"abc\", \"awsRegion\": \"eu-central-1\"}",
                "user-guide-url"))) {
            iterator.forEachRemaining(results::add);
        }
        final DocumentObject rootElement = (DocumentObject) results.get(0).getRootDocumentNode();
        final DocumentStringValue value = (DocumentStringValue) rootElement.get(KEY);
        assertThat(value.getValue(), equalTo(VALUE));
    }

    private static class Stub extends AbstractDynamodbDocumentFetcher {
        private static final long serialVersionUID = 1L;

        @Override
        protected Iterator<Map<String, AttributeValue>> run(final DynamoDbClient client) {
            return List.of(Map.of(KEY, AttributeValueQuickCreator.forString(VALUE))).iterator();
        }

        @Override
        protected String getTableName() {
            return MY_TABLE_NAME;
        }
    }
}