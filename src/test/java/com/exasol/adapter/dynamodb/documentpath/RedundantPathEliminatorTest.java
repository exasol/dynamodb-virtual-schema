package com.exasol.adapter.dynamodb.documentpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.junit.jupiter.api.Test;

class RedundantPathEliminatorTest {
    private static final DocumentPathExpression A = new DocumentPathExpression.Builder().addObjectLookup("a").build();
    private static final DocumentPathExpression AB = new DocumentPathExpression.Builder().addObjectLookup("a")
            .addObjectLookup("b").build();
    private static final DocumentPathExpression ABC = new DocumentPathExpression.Builder().addObjectLookup("a")
            .addObjectLookup("b").addObjectLookup("c").build();
    private static final DocumentPathExpression D = new DocumentPathExpression.Builder().addObjectLookup("d").build();

    @Test
    void testDuplicates() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(A, A)), containsInAnyOrder(A));
    }

    @Test
    void testA_AB() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(A, AB)), containsInAnyOrder(A));
    }

    @Test
    void testAB_A() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(AB, A)), containsInAnyOrder(A));
    }

    @Test
    void testABC_A() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(ABC, A)), containsInAnyOrder(A));
    }

    @Test
    void testABC_AB_A() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(ABC, AB, A)),
                containsInAnyOrder(A));
    }

    @Test
    void testA_ABC() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(A, ABC)), containsInAnyOrder(A));
    }

    @Test
    void testA_AB_ABC() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(A, AB, ABC)),
                containsInAnyOrder(A));
    }

    @Test
    void testAB_ABC() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(AB, ABC)),
                containsInAnyOrder(AB));
    }

    @Test
    void testA_D() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(A, D)), containsInAnyOrder(A, D));
    }

    @Test
    void testABC_D() {
        assertThat(RedundantPathEliminator.getInstance().removeRedundantPaths(List.of(ABC, D)),
                containsInAnyOrder(ABC, D));
    }
}