package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.DocumentObject;
import com.exasol.adapter.dynamodb.documentnode.DocumentValue;

/**
 * Tests for {@link DocumentPathWalker}
 */
public class DocumentPathWalkerTest {

    private static final MockValueNode NESTED_VALUE = new MockValueNode();
    private static final MockObjectNode TEST_OBJECT_NODE = new MockObjectNode(Map.of("key", NESTED_VALUE));
    private static final MockArrayNode TEST_ARRAY_NODE = new MockArrayNode(List.of(NESTED_VALUE));

    @Test
    void testWalkEmptyPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        final DocumentNode result = new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE);
        assertThat(result, equalTo(TEST_OBJECT_NODE));
    }

    @Test
    void testWalkObjectPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .build();
        final DocumentNode result = new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE);
        assertThat(result, equalTo(NESTED_VALUE));
    }

    @Test
    void testNotAnObject() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .addObjectLookup("key2").build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertAll(() -> assertThat(exception.getCurrentPath(), equalTo("/key")),
                () -> assertThat(exception.getMessage(), equalTo("Can't perform key lookup on non object. (requested key= key2) (current path= /key)")));
    }

    @Test
    void testUnknownProperty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("unknownKey")
                .build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertAll(() -> assertThat(exception.getCurrentPath(), equalTo("/")),
                () -> assertThat(exception.getMessage(), equalTo("The requested lookup key (unknownKey) is not present in this object. (current path= /)")));
    }

    @Test
    void testArrayLookup() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(0).build();
        final DocumentNode result = new DocumentPathWalker(pathExpression).walk(TEST_ARRAY_NODE);
        assertThat(result, equalTo(NESTED_VALUE));
    }

    @Test
    void testOutOfBoundsArrayLookup() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(10).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_ARRAY_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup: Index: 10 Size: 1 (current path= /)"));
    }

    @Test
    void testArrayLookupOnNonArray() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(10).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup on non array. (current path= /)"));
    }

    private static class MockObjectNode implements DocumentObject {
        private final Map<String, DocumentNode> value;

        private MockObjectNode(final Map<String, DocumentNode> value) {
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

    private static class MockArrayNode implements DocumentArray {
        private final List<DocumentNode> value;

        private MockArrayNode(final List<DocumentNode> value) {
            this.value = value;
        }

        @Override
        public List<DocumentNode> getValueList() {
            return this.value;
        }

        @Override
        public DocumentNode getValue(final int index) {
            return this.value.get(index);
        }
    }

    private static class MockValueNode implements DocumentValue {

    }
}
