package com.exasol.adapter.document.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.MockObjectNode;
import com.exasol.adapter.document.documentnode.MockValueNode;

class LinearDocumentPathWalkerTest {

    private static final MockValueNode NESTED_VALUE = new MockValueNode("value");
    private static final MockObjectNode TEST_OBJECT_NODE = new MockObjectNode(Map.of("key", NESTED_VALUE));

    @Test
    void testWalk() {
        final DocumentPathExpression pathExpression = DocumentPathExpression.builder().addObjectLookup("key")
                .build();
        final Optional<DocumentNode<Object>> result = new LinearDocumentPathWalker<>(pathExpression)
                .walkThroughDocument(TEST_OBJECT_NODE);
        assertThat(result.orElse(null), equalTo(NESTED_VALUE));
    }

    @Test
    void testNonLinearPath() {
        final DocumentPathExpression pathExpression = DocumentPathExpression.builder().addArrayAll().build();
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new LinearDocumentPathWalker<>(pathExpression));
        assertThat(exception.getMessage(), equalTo(
                "The given path is not a linear path. You can either remove the ArrayAllSegments from path or use a DocumentPathWalker."));
    }
}
