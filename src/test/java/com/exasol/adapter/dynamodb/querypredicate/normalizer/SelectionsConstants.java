package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import java.util.Set;

import com.exasol.adapter.dynamodb.documentnode.MockValueNode;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.querypredicate.AbstractComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.querypredicate.LogicalOperator;
import com.exasol.adapter.dynamodb.querypredicate.NotPredicate;

class SelectionsConstants {
    static final ColumnLiteralComparisonPredicate<Object> EQUAL1;
    static final ColumnLiteralComparisonPredicate<Object> EQUAL2;
    static final ColumnLiteralComparisonPredicate<Object> EQUAL3;
    static final LogicalOperator<Object> AND_OF_TWO_DIFFERENT_PREDICATES;
    static final LogicalOperator<Object> OR_OF_TWO_DIFFERENT_PREDICATES;
    static final LogicalOperator<Object> NESTED_AND;
    static final NotPredicate<Object> NOT_EQUAL1;

    static {
        EQUAL1 = new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                new ToJsonPropertyToColumnMapping("isbn", null, null), new MockValueNode("test"));
        EQUAL2 = new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                new ToJsonPropertyToColumnMapping("publisher", null, null), new MockValueNode("test"));

        EQUAL3 = new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                new ToJsonPropertyToColumnMapping("publisher", null, null), new MockValueNode("test2"));

        AND_OF_TWO_DIFFERENT_PREDICATES = new LogicalOperator<>(Set.of(EQUAL1, EQUAL2), LogicalOperator.Operator.AND);
        OR_OF_TWO_DIFFERENT_PREDICATES = new LogicalOperator<>(Set.of(EQUAL1, EQUAL2), LogicalOperator.Operator.OR);
        NESTED_AND = new LogicalOperator<>(
                Set.of(EQUAL3, new LogicalOperator<>(Set.of(EQUAL1, EQUAL2), LogicalOperator.Operator.AND)),
                LogicalOperator.Operator.OR);
        NOT_EQUAL1 = new NotPredicate<>(EQUAL1);
    }
}
