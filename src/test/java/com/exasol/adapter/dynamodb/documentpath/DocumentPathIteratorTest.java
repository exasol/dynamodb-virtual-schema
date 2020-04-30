package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.MockArrayNode;
import com.exasol.adapter.dynamodb.documentnode.MockObjectNode;
import com.exasol.adapter.dynamodb.documentnode.MockValueNode;

class DocumentPathIteratorTest {
    private static final String KEY = "key";
    private static final DocumentPathExpression SINGLE_NESTED_PATH = new DocumentPathExpression.Builder()
            .addObjectLookup(KEY).addArrayAll().build();

    @Test
    void testSimpleIteration() {
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final DocumentPathIterator iterator = new DocumentPathIteratorFactory<>().buildFor(SINGLE_NESTED_PATH,
                testDocument);
        int counter = 0;
        while (iterator.next()) {
            assertThat(iterator.getIndexFor(SINGLE_NESTED_PATH), equalTo(counter));
            counter++;
        }
        assertThat(counter, equalTo(2));
    }

    @Test
    void testEmptyIteration() {
        final MockObjectNode testDocument = new MockObjectNode(Map.of(KEY, new MockArrayNode(List.of())));
        final DocumentPathIterator iterator = new DocumentPathIteratorFactory<>().buildFor(SINGLE_NESTED_PATH,
                testDocument);
        int counter = 0;
        while (iterator.next()) {
            counter++;
        }
        assertThat(counter, equalTo(0));
    }

    @Test
    void testPathWithNoArrayAll() {
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final DocumentPathExpression pathWithNoArrayAll = new DocumentPathExpression.Builder().addObjectLookup("key")
                .build();
        final DocumentPathIterator iterator = new DocumentPathIteratorFactory<>().buildFor(pathWithNoArrayAll,
                testDocument);
        int counter = 0;
        while (iterator.next()) {
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
        final DocumentPathExpression doubleNestedPath = new DocumentPathExpression.Builder().addObjectLookup("key")
                .addArrayAll().addArrayAll().build();
        final DocumentPathIterator iterator = new DocumentPathIteratorFactory<>().buildFor(doubleNestedPath,
                testDocument);
        int counter = 0;
        final List<String> combinations = new ArrayList<>();
        while (iterator.next()) {
            counter++;
            final int index1 = iterator.getIndexFor(SINGLE_NESTED_PATH);
            final int index2 = iterator.getIndexFor(doubleNestedPath);
            combinations.add(index1 + "-" + index2);
        }
        assertThat(counter, equalTo(3));
        assertThat(combinations, containsInAnyOrder("0-0", "0-1", "1-0"));
    }

    @Test
    void testUnfittingPathForGetIndexFor() {
        final DocumentPathExpression otherPath = new DocumentPathExpression.Builder().addObjectLookup("other")
                .addArrayAll().build();
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final DocumentPathIterator iterator = new DocumentPathIteratorFactory<>().buildFor(SINGLE_NESTED_PATH,
                testDocument);
        iterator.next();
        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> iterator.getIndexFor(otherPath));
        assertThat(exception.getMessage(),
                equalTo("The requested path does not match the path that this iterator unwinds."));
    }

    @Test
    void testTooLongPathForGetIndexFor() {
        final DocumentPathExpression tooLongPath = new DocumentPathExpression.Builder().addObjectLookup(KEY)
                .addArrayAll().addArrayAll().build();
        final MockObjectNode testDocument = new MockObjectNode(
                Map.of(KEY, new MockArrayNode(List.of(new MockValueNode("value1"), new MockValueNode("value2")))));
        final DocumentPathIterator iterator = new DocumentPathIteratorFactory<>().buildFor(SINGLE_NESTED_PATH,
                testDocument);
        iterator.next();
        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> iterator.getIndexFor(tooLongPath));
        assertThat(exception.getMessage(), equalTo("The requested path is longer than the unwinded one."));
    }
}