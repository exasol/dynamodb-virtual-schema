package com.exasol.adapter.dynamodb.documentnode;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link DocumentNodeVisitor}.
 */
public class DocumentNodeVisitorTest {

    @Test
    void testVisitArray(){
        final MockDocumentNodeVisitor visitor = new MockDocumentNodeVisitor();
        final DocumentNode documentArray = new MockArrayNode(Collections.emptyList());
        documentArray.accept(visitor);
        assertThat(visitor.lastVisited, equalTo(VisitedType.Array));
    }

    @Test
    void testVisitObject(){
        final MockDocumentNodeVisitor visitor = new MockDocumentNodeVisitor();
        final DocumentNode documentArray = new MockObjectNode(Collections.emptyMap());
        documentArray.accept(visitor);
        assertThat(visitor.lastVisited, equalTo(VisitedType.Object));
    }

    @Test
    void testVisitValue(){
        final MockDocumentNodeVisitor visitor = new MockDocumentNodeVisitor();
        final DocumentNode documentArray = new MockValueNode();
        documentArray.accept(visitor);
        assertThat(visitor.lastVisited, equalTo(VisitedType.Value));
    }

    private static enum VisitedType{
        Array, Object, Value, Nothing
    }

    private static class MockDocumentNodeVisitor implements DocumentNodeVisitor{
        private VisitedType lastVisited = VisitedType.Nothing;
        @Override
        public void visit(final DocumentArray array) {
            this.lastVisited = VisitedType.Array;
        }

        @Override
        public void visit(final DocumentObject object) {
            this.lastVisited = VisitedType.Object;
        }

        @Override
        public void visit(final DocumentValue value) {
            this.lastVisited = VisitedType.Value;
        }
    }
}
