package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * Tests for {@link DocumentPathWalker}
 */
public class DocumentPathWalkerTest {

    private static final MockValueNode NESTED_VALUE1 = new MockValueNode();
    private static final MockValueNode NESTED_VALUE2 = new MockValueNode();
    private static final MockObjectNode TEST_OBJECT_NODE = new MockObjectNode(Map.of("key", NESTED_VALUE1));
    private static final MockArrayNode TEST_ARRAY_NODE = new MockArrayNode(List.of(NESTED_VALUE1, NESTED_VALUE2));

    @Test
    void testWalkEmptyPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().build();
        final List<DocumentNode> result = new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE);
        assertThat(result, containsInAnyOrder(TEST_OBJECT_NODE));
    }

    @Test
    void testWalkObjectPath() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .build();
        final List<DocumentNode> result = new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE);
        assertThat(result, containsInAnyOrder(NESTED_VALUE1));
    }

    @Test
    void testNotAnObject() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .addObjectLookup("key2").build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertAll(() -> assertThat(exception.getCurrentPath(), equalTo("/key")),
                () -> assertThat(exception.getMessage(),
                        equalTo("Can't perform key lookup on non object. (requested key= key2) (current path= /key)")));
    }

    @Test
    void testUnknownProperty() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("unknownKey")
                .build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertAll(() -> assertThat(exception.getCurrentPath(), equalTo("/")), () -> assertThat(exception.getMessage(),
                equalTo("The requested lookup key (unknownKey) is not present in this object. (current path= /)")));
    }

    @Test
    void testArrayLookup() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(0).build();
        final List<DocumentNode> result = new DocumentPathWalker(pathExpression).walk(TEST_ARRAY_NODE);
        assertThat(result, containsInAnyOrder(NESTED_VALUE1));
    }

    @Test
    void testOutOfBoundsArrayLookup() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(10).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_ARRAY_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup: Index: 10 Size: 2 (current path= /)"));
    }

    @Test
    void testArrayLookupOnNonArray() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayLookup(10).build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup on non array. (current path= /)"));
    }

    @Test
    void testArrayAll() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayAll().build();
        final List<DocumentNode> result = new DocumentPathWalker(pathExpression).walk(TEST_ARRAY_NODE);
        assertThat(result, containsInAnyOrder(NESTED_VALUE1, NESTED_VALUE2));
    }

    @Test
    void testArrayAllOnNonArray() throws DocumentPathWalkerException {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayAll().build();
        final DocumentPathWalkerException exception = assertThrows(DocumentPathWalkerException.class,
                () -> new DocumentPathWalker(pathExpression).walk(TEST_OBJECT_NODE));
        assertThat(exception.getMessage(), equalTo("Can't perform array lookup on non array. (current path= /)"));
    }
}
