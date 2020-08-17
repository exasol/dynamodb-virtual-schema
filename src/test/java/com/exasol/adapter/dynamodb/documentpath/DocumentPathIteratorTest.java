package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.MockArrayNode;
import com.exasol.adapter.dynamodb.documentnode.MockObjectNode;
import com.exasol.adapter.dynamodb.documentnode.MockValueNode;

class DocumentPathIteratorTest {
    private static final String KEY = "key";
    private static final DocumentPathExpression SINGLE_NESTED_PATH = DocumentPathExpression.builder()
            .addObjectLookup(KEY).addArrayAll().build();

    @Test
    void testSimpleIteration() {
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final DocumentPathIteratorFactory<Object> iterable = new DocumentPathIteratorFactory<>(SINGLE_NESTED_PATH,
                testDocument);
        int counter = 0;
        for (final PathIterationStateProvider state : iterable) {
            assertThat(state.getIndexFor(SINGLE_NESTED_PATH), equalTo(counter));
            counter++;
        }
        assertThat(counter, equalTo(2));
    }

    @Test
    void testEmptyIteration() {
        final MockObjectNode testDocument = new MockObjectNode(Map.of(KEY, new MockArrayNode(List.of())));
        final DocumentPathIteratorFactory<Object> iterable = new DocumentPathIteratorFactory<>(SINGLE_NESTED_PATH,
                testDocument);
        int counter = 0;
        for (final PathIterationStateProvider state : iterable) {
            counter++;
        }
        assertThat(counter, equalTo(0));
    }

    @Test
    void testNoMoreElementsException() {
        final MockObjectNode testDocument = new MockObjectNode(Map.of(KEY, new MockArrayNode(List.of())));
        final Iterator<PathIterationStateProvider> iterator = new DocumentPathIteratorFactory<>(SINGLE_NESTED_PATH,
                testDocument).iterator();
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testPathWithNoArrayAll() {
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final DocumentPathExpression pathWithNoArrayAll = DocumentPathExpression.builder().addObjectLookup("key")
                .build();
        final DocumentPathIteratorFactory<Object> iterable = new DocumentPathIteratorFactory<>(pathWithNoArrayAll,
                testDocument);
        int counter = 0;
        for (final PathIterationStateProvider state : iterable) {
            counter++;
        }
        assertThat(counter, equalTo(1));
    }

    @Test
    void testNestedIteration() {
        final MockObjectNode testDocument = new MockObjectNode(Map.of(KEY, new MockArrayNode(List.of(//
                new MockArrayNode(List.of(new MockValueNode("v1"), new MockValueNode("v2"))), //
                new MockArrayNode(List.of(new MockValueNode("v3")))//
        ))));
        final DocumentPathExpression doubleNestedPath = DocumentPathExpression.builder().addObjectLookup("key")
                .addArrayAll().addArrayAll().build();
        final DocumentPathIteratorFactory<Object> iterable = new DocumentPathIteratorFactory<>(doubleNestedPath,
                testDocument);
        int counter = 0;
        final List<String> combinations = new ArrayList<>();
        for (final PathIterationStateProvider state : iterable) {
            counter++;
            final int index1 = state.getIndexFor(SINGLE_NESTED_PATH);
            final int index2 = state.getIndexFor(doubleNestedPath);
            combinations.add(index1 + "-" + index2);
        }
        assertThat(counter, equalTo(3));
        assertThat(combinations, containsInAnyOrder("0-0", "0-1", "1-0"));
    }

    @Test
    void testUnfittingPathForGetIndexFor() {
        final DocumentPathExpression otherPath = DocumentPathExpression.builder().addObjectLookup("other")
                .addArrayAll().build();
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final Iterator<PathIterationStateProvider> iterator = new DocumentPathIteratorFactory<>(SINGLE_NESTED_PATH,
                testDocument).iterator();
        final PathIterationStateProvider next = iterator.next();
        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> next.getIndexFor(otherPath));
        assertThat(exception.getMessage(),
                equalTo("The requested path does not match the path that this iterator unwinds."));
    }

    @Test
    void testTooLongPathForGetIndexFor() {
        final DocumentPathExpression tooLongPath = DocumentPathExpression.builder().addObjectLookup(KEY)
                .addArrayAll().addArrayAll().build();
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final Iterator<PathIterationStateProvider> iterator = new DocumentPathIteratorFactory<>(SINGLE_NESTED_PATH,
                testDocument).iterator();
        final PathIterationStateProvider next = iterator.next();
        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> next.getIndexFor(tooLongPath));
        assertThat(exception.getMessage(), equalTo("The requested path is longer than the unwinded one."));
    }
}