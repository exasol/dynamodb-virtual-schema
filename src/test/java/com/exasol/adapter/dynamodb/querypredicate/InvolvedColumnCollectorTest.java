package com.exasol.adapter.dynamodb.querypredicate;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.sql.SqlLiteralString;

class InvolvedColumnCollectorTest {
    private static final InvolvedColumnCollector COLLECTOR = new InvolvedColumnCollector();
    private static final ToJsonPropertyToColumnMapping COLUMN1 = new ToJsonPropertyToColumnMapping("column1", null,
            null, 0, ToJsonPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
    private static final ToJsonPropertyToColumnMapping COLUMN2 = new ToJsonPropertyToColumnMapping("column2", null,
            null, 0, ToJsonPropertyToColumnMapping.OverflowBehaviour.EXCEPTION);
    private static final QueryPredicate COMPARISON1 = new ColumnLiteralComparisonPredicate(
            AbstractComparisonPredicate.Operator.EQUAL, COLUMN1, new SqlLiteralString(""));
    private static final QueryPredicate COMPARISON2 = new ColumnLiteralComparisonPredicate(
            AbstractComparisonPredicate.Operator.EQUAL, COLUMN2, new SqlLiteralString(""));

    @Test
    void testNoPredicate() {
        final List<ColumnMapping> columnMappings = COLLECTOR.collectInvolvedColumns(new NoPredicate());
        assertThat(columnMappings, is(emptyList()));
    }

    @Test
    void testComparison() {
        final List<ColumnMapping> columnMappings = COLLECTOR.collectInvolvedColumns(COMPARISON1);
        assertThat(columnMappings, containsInAnyOrder(COLUMN1));
    }

    @Test
    void testNotPredicate() {
        final List<ColumnMapping> columnMappings = COLLECTOR.collectInvolvedColumns(new NotPredicate(COMPARISON1));
        assertThat(columnMappings, containsInAnyOrder(COLUMN1));
    }

    @Test
    void testLogicalOperator() {
        final List<ColumnMapping> columnMappings = COLLECTOR.collectInvolvedColumns(
                new LogicalOperator(Set.of(COMPARISON1, COMPARISON2), LogicalOperator.Operator.AND));
        assertThat(columnMappings, containsInAnyOrder(COLUMN1, COLUMN2));
    }
}