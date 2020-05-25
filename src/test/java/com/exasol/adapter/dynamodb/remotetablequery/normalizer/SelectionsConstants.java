package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import java.util.List;

import com.exasol.adapter.dynamodb.documentnode.MockValueNode;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.ComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;

class SelectionsConstants {
    static final ColumnLiteralComparisonPredicate<Object> EQUAL1 = new ColumnLiteralComparisonPredicate<>(
            ComparisonPredicate.Operator.EQUAL, new ToJsonPropertyToColumnMapping("isbn", null, null),
            new MockValueNode("test"));
    static final ColumnLiteralComparisonPredicate<Object> EQUAL2 = new ColumnLiteralComparisonPredicate<>(
            ComparisonPredicate.Operator.EQUAL, new ToJsonPropertyToColumnMapping("publisher", null, null),
            new MockValueNode("test"));
    static final ColumnLiteralComparisonPredicate<Object> EQUAL3 = new ColumnLiteralComparisonPredicate<>(
            ComparisonPredicate.Operator.EQUAL, new ToJsonPropertyToColumnMapping("publisher", null, null),
            new MockValueNode("test2"));
    static final LogicalOperator<Object> AND_OF_TWO_DIFFERENT_PREDICATES = new LogicalOperator<>(
            List.of(EQUAL1, EQUAL2), LogicalOperator.Operator.AND);
    static final LogicalOperator<Object> AND_OF_TWO_IDENTICAL_PREDICATES = new LogicalOperator<>(
            List.of(EQUAL1, EQUAL1), LogicalOperator.Operator.AND);
    static final LogicalOperator<Object> OR_OF_TWO_DIFFERENT_PREDICATES = new LogicalOperator<>(List.of(EQUAL1, EQUAL2),
            LogicalOperator.Operator.OR);
    static final LogicalOperator<Object> NESTED_AND = new LogicalOperator<>(
            List.of(EQUAL3, new LogicalOperator<>(List.of(EQUAL1, EQUAL2), LogicalOperator.Operator.AND)),
            LogicalOperator.Operator.OR);
}
