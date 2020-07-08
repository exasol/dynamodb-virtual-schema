package com.exasol.adapter.dynamodb.querypredicate;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.LookupFailBehaviour;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.sql.SqlLiteralString;

class ColumnLiteralComparisonPredicateTest {
    private static final AbstractComparisonPredicate.Operator OPERATOR = AbstractComparisonPredicate.Operator.EQUAL;
    private static final SqlLiteralString LITERAL = new SqlLiteralString("test");
    private static final ColumnMapping COLUMN = new ToJsonPropertyToColumnMapping("", DocumentPathExpression.empty(),
            LookupFailBehaviour.EXCEPTION, 0, ToJsonPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
    private static final ColumnLiteralComparisonPredicate TEST_PREDICATE = new ColumnLiteralComparisonPredicate(
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
        assertThat(visitor.getVisited(), equalTo(PredicateTestVisitor.Visited.COMPARISON));
    }

    @Test
    void testIdentical() {
        assertSymmetricEqualWithHashAndEquals(TEST_PREDICATE, TEST_PREDICATE);
    }

    @Test
    void testEqual() {
        final ColumnLiteralComparisonPredicate otherPredicate = new ColumnLiteralComparisonPredicate(OPERATOR,
                COLUMN, LITERAL);
        assertSymmetricEqualWithHashAndEquals(TEST_PREDICATE, otherPredicate);
    }
}