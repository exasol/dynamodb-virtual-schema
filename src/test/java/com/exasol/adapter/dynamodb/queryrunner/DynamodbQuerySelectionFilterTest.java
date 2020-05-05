package com.exasol.adapter.dynamodb.queryrunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

class DynamodbQuerySelectionFilterTest {
    private static final String WHITELISTED_NAME = "nameThatIsOnWhitelist";
    private static final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> WHITELISTED_PREDICATE = TestSetup
            .getCompareForColumn(WHITELISTED_NAME);
    private static final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> NON_WHITELISTED_PREDICATE = TestSetup
            .getCompareForColumn("nameNotOnWhitelist");

    @Test
    void testWhitelistedColumnIsNotFiltered() throws PlanDoesNotFitException {
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter()
                .filter(WHITELISTED_PREDICATE, List.of(WHITELISTED_NAME));
        assertThat(result, equalTo(WHITELISTED_PREDICATE));
    }

    @Test
    void testNonWhitelistedColumnIsFiltered() throws PlanDoesNotFitException {
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter()
                .filter(WHITELISTED_PREDICATE, List.of("other key"));
        assertThat(result, equalTo(new NoPredicate<>()));
    }

    @Test
    void testFilterAndWithTwoResults() throws PlanDoesNotFitException {
        final LogicalOperator<DynamodbNodeVisitor> and = new LogicalOperator<>(
                List.of(WHITELISTED_PREDICATE, WHITELISTED_PREDICATE, NON_WHITELISTED_PREDICATE),
                LogicalOperator.Operator.AND);
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter().filter(and,
                List.of(WHITELISTED_NAME));
        final LogicalOperator<DynamodbNodeVisitor> compareResult = (LogicalOperator<DynamodbNodeVisitor>) result;
        assertThat(compareResult.getOperands(), containsInAnyOrder(WHITELISTED_PREDICATE, WHITELISTED_PREDICATE));
    }

    @Test
    void testFilterAndWithOneResult() throws PlanDoesNotFitException {
        final LogicalOperator<DynamodbNodeVisitor> and = new LogicalOperator<>(
                List.of(WHITELISTED_PREDICATE, NON_WHITELISTED_PREDICATE), LogicalOperator.Operator.AND);
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter().filter(and,
                List.of(WHITELISTED_NAME));
        assertThat(result, equalTo(WHITELISTED_PREDICATE));
    }

    @Test
    void testFilterAndWithNoResults() throws PlanDoesNotFitException {
        final LogicalOperator<DynamodbNodeVisitor> and = new LogicalOperator<>(List.of(NON_WHITELISTED_PREDICATE),
                LogicalOperator.Operator.AND);
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter().filter(and,
                List.of(WHITELISTED_NAME));
        assertThat(result, equalTo(new NoPredicate<>()));
    }

    @Test
    void testFilterOrWithOnlyWhitelisted() throws PlanDoesNotFitException {
        final LogicalOperator<DynamodbNodeVisitor> or = new LogicalOperator<>(List.of(WHITELISTED_PREDICATE),
                LogicalOperator.Operator.OR);
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter().filter(or,
                List.of(WHITELISTED_NAME));
        assertThat(result, equalTo(WHITELISTED_PREDICATE));
    }

    @Test
    void testFilterOrWithNotOnlyWhitelisted() throws PlanDoesNotFitException {
        final LogicalOperator<DynamodbNodeVisitor> or = new LogicalOperator<>(
                List.of(WHITELISTED_PREDICATE, NON_WHITELISTED_PREDICATE), LogicalOperator.Operator.OR);
        final DynamodbQuerySelectionFilterException exception = assertThrows(
                DynamodbQuerySelectionFilterException.class,
                () -> new DynamodbQuerySelectionFilter().filter(or, List.of(WHITELISTED_NAME)));
        assertThat(exception.getMessage(), equalTo(
                "The key predicates of this plan could not be extracted without potentially loosing results of this query. Please simplify the query or use a different DynamoDB operation."));
    }

    @Test
    void testFilterNoPredicate() throws PlanDoesNotFitException {
        final QueryPredicate<DynamodbNodeVisitor> result = new DynamodbQuerySelectionFilter()
                .filter(new NoPredicate<>(), List.of(WHITELISTED_NAME));
        assertThat(result, equalTo(new NoPredicate<>()));
    }
}