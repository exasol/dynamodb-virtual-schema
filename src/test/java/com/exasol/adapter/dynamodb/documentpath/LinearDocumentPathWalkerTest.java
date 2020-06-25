package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.MockObjectNode;
import com.exasol.adapter.dynamodb.documentnode.MockValueNode;

class LinearDocumentPathWalkerTest {

    private static final MockValueNode NESTED_VALUE = new MockValueNode("value");
    private static final MockObjectNode TEST_OBJECT_NODE = new MockObjectNode(Map.of("key", NESTED_VALUE));

    @Test
    void testWalk() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addObjectLookup("key")
                .build();
        final DocumentNode<Object> result = new LinearDocumentPathWalker<Object>(pathExpression)
                .walkThroughDocument(TEST_OBJECT_NODE);
        assertThat(result, equalTo(NESTED_VALUE));
    }

    @Test
    void testNonLinearPath() {
        final DocumentPathExpression pathExpression = new DocumentPathExpression.Builder().addArrayAll().build();
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new LinearDocumentPathWalker<Object>(pathExpression));
        assertThat(exception.getMessage(), equalTo(
                "The given path is not a linear path. You can either remove the ArrayAllSegments from path or use a DocumentPathWalker."));
    }
}
