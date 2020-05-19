package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.dynamodb.remotetablequery.*;

class DynamodbFilterExpressionFactoryTest {

    @Test
    void testEqualityComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.EQUAL);
        assertFilterExpression(predicate, "key = :0", Map.of(":0", literal));
    }

    @Test
    void testLessComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.LESS);
        assertFilterExpression(predicate, "key < :0", Map.of(":0", literal));
    }

    @Test
    void testLessEqualComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.LESS_EQUAL);
        assertFilterExpression(predicate, "key <= :0", Map.of(":0", literal));
    }

    @Test
    void testGreaterComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.GREATER);
        assertFilterExpression(predicate, "key > :0", Map.of(":0", literal));
    }

    @Test
    void testGreaterEqualComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.GREATER_EQUAL);
        assertFilterExpression(predicate, "key >= :0", Map.of(":0", literal));
    }

    @Test
    void testAndWithTwoOperands() {
        final String literal1 = "test1";
        final String literal2 = "test2";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> comparison1 = getComparison(literal1,
                ComparisonPredicate.Operator.EQUAL);
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> comparison2 = getComparison(literal2,
                ComparisonPredicate.Operator.EQUAL);
        final LogicalOperator<DynamodbNodeVisitor> and = new LogicalOperator<>(List.of(comparison1, comparison2),
                LogicalOperator.Operator.AND);
        assertFilterExpression(and, "key = :0 and key = :1", Map.of(":0", literal1, ":1", literal2));
    }

    @Test
    void testAndWithThreeOperands() {
        final String literal1 = "test1";
        final String literal2 = "test2";
        final String literal3 = "test3";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> comparison1 = getComparison(literal1,
                ComparisonPredicate.Operator.EQUAL);
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> comparison2 = getComparison(literal2,
                ComparisonPredicate.Operator.EQUAL);
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> comparison3 = getComparison(literal3,
                ComparisonPredicate.Operator.EQUAL);
        final LogicalOperator<DynamodbNodeVisitor> and = new LogicalOperator<>(
                List.of(comparison1, comparison2, comparison3), LogicalOperator.Operator.AND);
        assertFilterExpression(and, "key = :0 and (key = :1 and key = :2)",
                Map.of(":0", literal1, ":1", literal2, ":2", literal3));
    }

    @Test
    void testNoPredicate() {
        assertFilterExpression(new NoPredicate<>(), "", Collections.emptyMap());
    }

    private ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> getComparison(final String literal,
            final ComparisonPredicate.Operator operator) {
        final DocumentPathExpression sourcePath = new DocumentPathExpression.Builder().addObjectLookup("key").build();
        final ToJsonPropertyToColumnMapping column = new ToJsonPropertyToColumnMapping("columnName", sourcePath, null);
        return new ColumnLiteralComparisonPredicate<>(operator, column, new DynamodbString(literal));
    }

    private void assertFilterExpression(final QueryPredicate<DynamodbNodeVisitor> predicateToTest,
            final String expectedExpression, final Map<String, String> expectedValueMap) {
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(predicateToTest,
                valueListBuilder);
        final Map<String, String> valuesStrings = valueListBuilder.getValueMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getS()));
        assertAll(() -> assertThat(result, equalTo(expectedExpression)),
                () -> assertThat(valuesStrings, equalTo(expectedValueMap))//
        );
    }
}
