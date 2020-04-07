package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * Tests for {@link DocumentPathWalker}
 */
public class DocumentPathWalkerTest {

    private static final MocValueNode NESTED_VALUE = new MocValueNode();
    private static final MocObjectNode TEST_OBJECT_NODE = new MocObjectNode(Map.of("key", NESTED_VALUE));

    @Test
    void testWalkEmptyPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        final DocumentNode result = new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE);
        assertThat(result, equalTo(TEST_OBJECT_NODE));
    }

    @Test
    void testWalkObjectPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()
                .add(new ObjectPathSegment("key")).build();
        final DocumentNode result = new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE);
        assertThat(result, equalTo(NESTED_VALUE));
    }

    @Test
    void testNotAnObject() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()
                .add(new ObjectPathSegment("key")).add(new ObjectPathSegment("key2")).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertThat(exception.getCurrentPath(), equalTo("/key/"));
    }

    @Test
    void testUnknownProperty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder()
                .add(new ObjectPathSegment("unknownKey")).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertThat(exception.getCurrentPath(), equalTo("/"));
    }

    private static class MocObjectNode implements DocumentObject {
        private final Map<String, DocumentNode> value;

        private MocObjectNode(final Map<String, DocumentNode> value) {
            this.value = value;
        }

        @Override
        public Map<String, DocumentNode> getKeyValueMap() {
            return this.value;
        }

        @Override
        public DocumentNode get(final String key) {
            return this.value.get(key);
        }

        @Override
        public boolean hasKey(final String key) {
            return this.value.containsKey(key);
        }
    }

    private static class MocValueNode implements DocumentValue {

    }
}
