package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.ComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;

class DynamodbFilterExpressionFactoryTest {

    @Test
    void testEqualityComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.EQUAL);
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(predicate, valueListBuilder);
        assertThat(result, equalTo("key = :0"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal));
    }

    @Test
    void testLessComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.LESS);
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(predicate, valueListBuilder);
        assertThat(result, equalTo("key < :0"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal));
    }

    @Test
    void testLessEqualComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.LESS_EQUAL);
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(predicate, valueListBuilder);
        assertThat(result, equalTo("key <= :0"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal));
    }

    @Test
    void testGreaterComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.GREATER);
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(predicate, valueListBuilder);
        assertThat(result, equalTo("key > :0"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal));
    }

    @Test
    void testGreaterEqualComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> predicate = getComparison(literal,
                ComparisonPredicate.Operator.GREATER_EQUAL);
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(predicate, valueListBuilder);
        assertThat(result, equalTo("key >= :0"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal));
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
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(and, valueListBuilder);
        assertThat(result, equalTo("key = :0 and key = :1"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal1));
        assertThat(valueListBuilder.getValueMap().get(":1").getS(), equalTo(literal2));
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
        final DynamodbValueListBuilder valueListBuilder = new DynamodbValueListBuilder();
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(and, valueListBuilder);
        assertThat(result, equalTo("key = :0 and (key = :1 and key = :2)"));
        assertThat(valueListBuilder.getValueMap().get(":0").getS(), equalTo(literal1));
        assertThat(valueListBuilder.getValueMap().get(":1").getS(), equalTo(literal2));
        assertThat(valueListBuilder.getValueMap().get(":2").getS(), equalTo(literal3));
    }

    @Test
    void testNoPredicate() {
        final String result = new DynamodbFilterExpressionFactory().buildFilterExpression(new NoPredicate<>(),
                new DynamodbValueListBuilder());
        assertThat(result, equalTo(""));
    }

    @NotNull
    private ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> getComparison(final String literal,
            final ComparisonPredicate.Operator operator) {
        final DocumentPathExpression sourcePath = new DocumentPathExpression.Builder().addObjectLookup("key").build();
        final ToJsonColumnMappingDefinition column = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("columnName", sourcePath, null));
        return new ColumnLiteralComparisonPredicate<>(operator, column, new DynamodbString(literal));
    }
}
