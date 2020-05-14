package com.exasol.adapter.dynamodb.remotetablequery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;

class ColumnLiteralComparisonPredicateTest {
    private static final ComparisonPredicate.Operator OPERATOR = ComparisonPredicate.Operator.EQUAL;
    private static final DocumentValue<Object> LITERAL = (DocumentValue<Object>) visitor -> {
    };
    private static final ColumnMapping COLUMN = new ToJsonPropertyToColumnMapping(null, null, null);
    private static final ColumnLiteralComparisonPredicate<Object> TEST_PREDICATE = new ColumnLiteralComparisonPredicate<>(
            OPERATOR, COLUMN, LITERAL);

    @Test
    void testGetColumn() {
        assertThat(TEST_PREDICATE.getColumn(), equalTo(COLUMN));
    }

    @Test
    void testGetLiteral() {
        assertThat(TEST_PREDICATE.getLiteral(), equalTo(LITERAL));
    }

    @Test
    void testGetOperator() {
        assertThat(TEST_PREDICATE.getOperator(), equalTo(OPERATOR));
    }

    @Test
    void testVisitor() {
        final PredicateTestVisitor visitor = new PredicateTestVisitor();
        TEST_PREDICATE.accept(visitor);
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.COLUMN_LITERAL_COMPARISON));
    }

    @Test
    void testNotEqual() {
        final ColumnLiteralComparisonPredicate<Object> other = new ColumnLiteralComparisonPredicate<>(OPERATOR, COLUMN,
                (DocumentValue<Object>) visitor -> {
                });
        assertThat(TEST_PREDICATE, not(equalTo(other)));
    }

    @Test
    void testEqual() {
        assertThat(TEST_PREDICATE, equalTo(TEST_PREDICATE));
    }

    @Test
    void testHashCodeEqual() {
        assertThat(TEST_PREDICATE.hashCode(), equalTo(TEST_PREDICATE.hashCode()));
    }

    @Test
    void testHashCodeNotEqual() {
        final ColumnLiteralComparisonPredicate<Object> other = new ColumnLiteralComparisonPredicate<>(OPERATOR, COLUMN,
                (DocumentValue<Object>) visitor -> {
                });
        assertThat(TEST_PREDICATE.hashCode(), not(equalTo(other.hashCode())));
    }
}