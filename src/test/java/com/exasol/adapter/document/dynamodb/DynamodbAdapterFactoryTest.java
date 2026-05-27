package com.exasol.adapter.document.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.document.DocumentAdapter;

class DynamodbAdapterFactoryTest {
    private final DynamodbAdapterFactory factory = new DynamodbAdapterFactory();

    @Test
    void testCreateAdapterReturnsDocumentAdapter() {
        final VirtualSchemaAdapter adapter = this.factory.createAdapter(null);
        assertThat(adapter, instanceOf(DocumentAdapter.class));
    }

    @Test
    void testGetAdapterProjectShortTag() {
        assertThat(this.factory.getAdapterProjectShortTag(), equalTo("VSDY"));
    }

    @Test
    void testgetAdapterName() {
        assertThat(this.factory.getAdapterName(), equalTo(DynamodbDocumentAdapterDialect.ADAPTER_NAME));
    }

    @Test
    void testGetAdapterVersion() {
        // Version only available in built artifact
        assertThat(this.factory.getAdapterVersion(), equalTo("UNKNOWN"));
    }
}
