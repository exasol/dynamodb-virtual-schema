package com.exasol.adapter.dynamodb.remotetablequery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ToJsonColumnMappingDefinition;

class ColumnLiteralComparisonPredicateTest {
    private static final ComparisonPredicate.Operator OPERATOR = ComparisonPredicate.Operator.EQUAL;
    private static final DocumentValue<Object> LITERAL = (DocumentValue<Object>) visitor -> {
    };
    private static final AbstractColumnMappingDefinition COLUMN = new ToJsonColumnMappingDefinition(
            new AbstractColumnMappingDefinition.ConstructorParameters(null, null, null));
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
}