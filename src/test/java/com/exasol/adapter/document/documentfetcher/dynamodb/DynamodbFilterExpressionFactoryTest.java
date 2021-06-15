package com.exasol.adapter.document.documentfetcher.dynamodb;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.getColumnMappingExample;
import static com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator.EQUAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.document.documentpath.DocumentPathExpression;
import com.exasol.adapter.document.mapping.PropertyToColumnMapping;
import com.exasol.adapter.document.mapping.SourceReferenceColumnMapping;
import com.exasol.adapter.document.querypredicate.*;
import com.exasol.adapter.sql.SqlLiteralString;

class DynamodbFilterExpressionFactoryTest {

    @Test
    void testEqualityComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate predicate = getComparison(literal, EQUAL);
        assertFilterExpression(predicate, "#0 = :0", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testInequalityComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate predicate = getComparison(literal,
                AbstractComparisonPredicate.Operator.NOT_EQUAL);
        assertFilterExpression(predicate, "#0 <> :0", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testNot() {
        final String literal = "test";
        final NotPredicate predicate = new NotPredicate(getComparison(literal, EQUAL));
        assertFilterExpression(predicate, "NOT (#0 = :0)", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testLessComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate predicate = getComparison(literal,
                AbstractComparisonPredicate.Operator.LESS);
        assertFilterExpression(predicate, "#0 < :0", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testLessEqualComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate predicate = getComparison(literal,
                AbstractComparisonPredicate.Operator.LESS_EQUAL);
        assertFilterExpression(predicate, "#0 <= :0", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testGreaterComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate predicate = getComparison(literal,
                AbstractComparisonPredicate.Operator.GREATER);
        assertFilterExpression(predicate, "#0 > :0", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testGreaterEqualComparison() {
        final String literal = "test";
        final ColumnLiteralComparisonPredicate predicate = getComparison(literal,
                AbstractComparisonPredicate.Operator.GREATER_EQUAL);
        assertFilterExpression(predicate, "#0 >= :0", Map.of("#0", "key"), Map.of(":0", literal));
    }

    @Test
    void testAndWithTwoOperands() {
        final String literal1 = "test1";
        final String literal2 = "test2";
        final ColumnLiteralComparisonPredicate comparison1 = getComparison(literal1, EQUAL);
        final ColumnLiteralComparisonPredicate comparison2 = getComparison(literal2, EQUAL);
        final LogicalOperator and = new LogicalOperator(Set.of(comparison1, comparison2), LogicalOperator.Operator.AND);
        assertFilterExpression(and, "(#0 = :0 and (#0 = :1))", Set.of("key"), Set.of(literal1, literal2));
    }

    @Test
    void testAndWithThreeOperands() {
        final String literal1 = "test1";
        final String literal2 = "test2";
        final String literal3 = "test3";
        final ColumnLiteralComparisonPredicate comparison1 = getComparison(literal1, EQUAL);
        final ColumnLiteralComparisonPredicate comparison2 = getComparison(literal2, EQUAL);
        final ColumnLiteralComparisonPredicate comparison3 = getComparison(literal3, EQUAL);
        final LogicalOperator and = new LogicalOperator(Set.of(comparison1, comparison2, comparison3),
                LogicalOperator.Operator.AND);
        assertFilterExpression(and, "(#0 = :0 and ((#0 = :1 and (#0 = :2))))", Set.of("key"),
                Set.of(literal1, literal2, literal3));
    }

    @Test
    void testNonDynamoDBColumn() {
        final ColumnLiteralComparisonPredicate comparison = getComparisonOnUnsupportedColumn();
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final DynamodbFilterExpressionFactory factory = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder,
                valuePlaceholderMapBuilder);
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> factory.buildFilterExpression(comparison));
        assertThat(exception.getMessage(), equalTo(
                "E-VS-DY-9: The column 'SOURCE_REFERENCE' has no corresponding DynamoDB column. Hence it can't be part of a filter expression."));
    }

    @Test
    void testCanNotConvert() {
        final ColumnLiteralComparisonPredicate comparison = getComparisonOnUnsupportedColumn();
        assertThat(DynamodbFilterExpressionFactory.canConvert(comparison), equalTo(false));
    }

    @Test
    void testCanConvert() {
        assertThat(DynamodbFilterExpressionFactory.canConvert(getComparison("test", EQUAL)), equalTo(true));
    }

    private ColumnLiteralComparisonPredicate getComparisonOnUnsupportedColumn() {
        final SourceReferenceColumnMapping column = new SourceReferenceColumnMapping();
        return new ColumnLiteralComparisonPredicate(EQUAL, column, new SqlLiteralString(""));
    }

    @Test
    void testNoPredicate() {
        assertFilterExpression(new NoPredicate(), "", Collections.emptyMap(), Collections.emptyMap());
    }

    private ColumnLiteralComparisonPredicate getComparison(final String literal,
            final AbstractComparisonPredicate.Operator operator) {
        final DocumentPathExpression sourcePath = DocumentPathExpression.builder().addObjectLookup("key").build();
        final PropertyToColumnMapping column = getColumnMappingExample().pathToSourceProperty(sourcePath).build();
        return new ColumnLiteralComparisonPredicate(operator, column, new SqlLiteralString(literal));
    }

    void assertFilterExpression(final QueryPredicate predicateToTest, final String expectedExpression,
            final Map<String, String> expectedAttributeNameMap, final Map<String, String> expectedValueMap) {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String result = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder, valuePlaceholderMapBuilder)
                .buildFilterExpression(predicateToTest);
        final Map<String, String> valuesStrings = valuePlaceholderMapBuilder.getPlaceholderMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> ((DynamodbString) entry.getValue()).getValue()));
        assertAll(//
                () -> assertThat(result, equalTo(expectedExpression)),
                () -> assertThat(valuesStrings, equalTo(expectedValueMap)),
                () -> assertThat(namePlaceholderMapBuilder.getPlaceholderMap(), equalTo(expectedAttributeNameMap))//
        );
    }

    void assertFilterExpression(final QueryPredicate predicateToTest, final String expectedExpression,
            final Set<String> expectedAttributeNames, final Set<String> expectedValues) {
        final DynamodbAttributeNamePlaceholderMapBuilder namePlaceholderMapBuilder = new DynamodbAttributeNamePlaceholderMapBuilder();
        final DynamodbAttributeValuePlaceholderMapBuilder valuePlaceholderMapBuilder = new DynamodbAttributeValuePlaceholderMapBuilder();
        final String result = new DynamodbFilterExpressionFactory(namePlaceholderMapBuilder, valuePlaceholderMapBuilder)
                .buildFilterExpression(predicateToTest);
        final Map<String, String> valuesStrings = valuePlaceholderMapBuilder.getPlaceholderMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> ((DynamodbString) entry.getValue()).getValue()));
        assertAll(//
                () -> assertThat(result, equalTo(expectedExpression)),
                () -> assertThat(new HashSet<>(valuesStrings.values()), equalTo(expectedValues)),
                () -> assertThat(new HashSet<>(namePlaceholderMapBuilder.getPlaceholderMap().values()),
                        equalTo(expectedAttributeNames))//
        );
    }
}
