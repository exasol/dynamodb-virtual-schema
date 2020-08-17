package com.exasol.adapter.document.querypredicate.normalizer;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.getColumnMappingExample;

import java.util.Set;

import com.exasol.adapter.document.mapping.MappingErrorBehaviour;
import com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate;
import com.exasol.adapter.document.querypredicate.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.document.querypredicate.LogicalOperator;
import com.exasol.adapter.document.querypredicate.NotPredicate;
import com.exasol.adapter.sql.SqlLiteralString;

class SelectionsConstants {
    static final ColumnLiteralComparisonPredicate EQUAL1;
    static final ColumnLiteralComparisonPredicate EQUAL2;
    static final ColumnLiteralComparisonPredicate EQUAL3;
    static final LogicalOperator AND_OF_TWO_DIFFERENT_PREDICATES;
    static final LogicalOperator OR_OF_TWO_DIFFERENT_PREDICATES;
    static final LogicalOperator NESTED_AND;
    static final NotPredicate NOT_EQUAL1;

    static {
        EQUAL1 = new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                getColumnMappingExample().exasolColumnName("isbn").build(), new SqlLiteralString("test"));
        EQUAL2 = new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                getColumnMappingExample().exasolColumnName("publisher").lookupFailBehaviour(MappingErrorBehaviour.ABORT)
                        .build(),
                new SqlLiteralString("test"));

        EQUAL3 = new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                getColumnMappingExample().exasolColumnName("publisher").lookupFailBehaviour(MappingErrorBehaviour.NULL)
                        .build(),
                new SqlLiteralString("test2"));

        AND_OF_TWO_DIFFERENT_PREDICATES = new LogicalOperator(Set.of(EQUAL1, EQUAL2), LogicalOperator.Operator.AND);
        OR_OF_TWO_DIFFERENT_PREDICATES = new LogicalOperator(Set.of(EQUAL1, EQUAL2), LogicalOperator.Operator.OR);
        NESTED_AND = new LogicalOperator(
                Set.of(EQUAL3, new LogicalOperator(Set.of(EQUAL1, EQUAL2), LogicalOperator.Operator.AND)),
                LogicalOperator.Operator.OR);
        NOT_EQUAL1 = new NotPredicate(EQUAL1);
    }
}
