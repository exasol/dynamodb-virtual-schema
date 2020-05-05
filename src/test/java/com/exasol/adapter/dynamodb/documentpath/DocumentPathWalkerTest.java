package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.MockArrayNode;
import com.exasol.adapter.dynamodb.documentnode.MockObjectNode;
import com.exasol.adapter.dynamodb.documentnode.MockValueNode;

public class DocumentPathWalkerTest {

    private static final MockValueNode NESTED_VALUE1 = new MockValueNode("value");
    private static final MockValueNode NESTED_VALUE2 = new MockValueNode("value");
    private static final String ARRAY_KEY = "array_key";
    private static final String OBJECT_KEY = "array_key";
    private static final MockArrayNode TEST_ARRAY_NODE = new MockArrayNode(List.of(NESTED_VALUE1, NESTED_VALUE2));
    private static final MockObjectNode TEST_OBJECT_NODE = new MockObjectNode(
            Map.of("key", NESTED_VALUE1, OBJECT_KEY, TEST_ARRAY_NODE));
    private static final MockObjectNode TEST_NESTED_OBJECT_NODE = new MockObjectNode(
            Map.of(OBJECT_KEY, TEST_OBJECT_NODE));

    @Test
    void testWalkEmptyPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        final DocumentNode<Object> result = new DocumentPathWalker<Object>(pathExpression,
                new StaticDocumentPathIterator()).walkThroughDocument(TEST_OBJECT_NODE);
        assertThat(result, equalTo(TEST_OBJECT_NODE));
    }

    @Test
    void testWalkObjectPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .build();
        final DocumentNode<Object> result = new DocumentPathWalker<Object>(pathExpression,
                new StaticDocumentPathIterator()).walkThroughDocument(TEST_OBJECT_NODE);
        assertThat(result, equalTo(NESTED_VALUE1));
    }

    @Test
    void testNestedObject() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup(OBJECT_KEY)
                .addObjectLookup("key").build();
        final DocumentNode<Object> result = new DocumentPathWalker<Object>(pathExpression,
                new StaticDocumentPathIterator()).walkThroughDocument(TEST_NESTED_OBJECT_NODE);
        assertThat(result, equalTo(NESTED_VALUE1));
    }

    @Test
    void testNotAnObject() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .addObjectLookup("key2").build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker<Object>(pathExpression, new StaticDocumentPathIterator())
                        .walkThroughDocument(TEST_OBJECT_NODE));
        assertAll(() -> assertThat(exception.getCurrentPath(), equalTo("/key")),
                () -> assertThat(exception.getMessage(),
                        equalTo("Can't perform key lookup on non object. (requested key= key2) (current path= /key)")));
    }

    @Test
    void testUnknownProperty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("unknownKey")
                .build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker<Object>(pathExpression, new StaticDocumentPathIterator())
                        .walkThroughDocument(TEST_OBJECT_NODE));
        assertAll(() -> assertThat(exception.getCurrentPath(), equalTo("/")), () -> assertThat(exception.getMessage(),
                equalTo("The requested lookup key (unknownKey) is not present in this object. (current path= /)")));
    }

    @Test
    void testArrayLookup() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(0).build();
        final DocumentNode<Object> result = new DocumentPathWalker<Object>(pathExpression,
                new StaticDocumentPathIterator()).walkThroughDocument(TEST_ARRAY_NODE);
        assertThat(result, equalTo(NESTED_VALUE1));
    }

    @Test
    void testOutOfBoundsArrayLookup() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(10).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker<Object>(pathExpression, new StaticDocumentPathIterator())
                        .walkThroughDocument(TEST_ARRAY_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup: Index: 10 Size: 2 (current path= /)"));
    }

    @Test
    void testArrayLookupOnNonArray() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(10).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker<Object>(pathExpression, new StaticDocumentPathIterator())
                        .walkThroughDocument(TEST_OBJECT_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup on non array. (current path= /)"));
    }

    @Test
    void testArrayAll() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayAll().build();
        final DocumentNode<Object> result1 = new DocumentPathWalker<Object>(pathExpression,
                new PathIterationStateProviderStub(0)).walkThroughDocument(TEST_ARRAY_NODE);
        final DocumentNode<Object> result2 = new DocumentPathWalker<Object>(pathExpression,
                new PathIterationStateProviderStub(1)).walkThroughDocument(TEST_ARRAY_NODE);
        assertAll(() -> assertThat(result1, equalTo(NESTED_VALUE1)), () -> assertThat(result2, equalTo(NESTED_VALUE2)));
    }

    @Test
    void testNestedArrayAll() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup(OBJECT_KEY)
                .addArrayAll().build();
        final DocumentNode<Object> result1 = new DocumentPathWalker<Object>(pathExpression,
                new PathIterationStateProviderStub(0)).walkThroughDocument(TEST_OBJECT_NODE);
        final DocumentNode<Object> result2 = new DocumentPathWalker<Object>(pathExpression,
                new PathIterationStateProviderStub(1)).walkThroughDocument(TEST_OBJECT_NODE);
        assertAll(() -> assertThat(result1, equalTo(NESTED_VALUE1)), () -> assertThat(result2, equalTo(NESTED_VALUE2)));
    }

    private static class PathIterationStateProviderStub implements PathIterationStateProvider {
        final private int index;

        private PathIterationStateProviderStub(final int index) {
            this.index = index;
        }

        @Override
        public int getIndexFor(final DocumentPathExpression pathToArrayAll) {
            return this.index;
        }
    }
}