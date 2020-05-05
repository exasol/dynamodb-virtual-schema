package com.exasol.adapter.dynamodb.queryrunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.ComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;

class DynamodbQuerySelectionRaterTest {
    private static final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> EQUAL = new ColumnLiteralComparisonPredicate<>(
            ComparisonPredicate.Operator.EQUAL, null, null);

    @Test
    void testNoPredicate() {
        final int rating = new DynamodbQuerySelectionRater().rate(new NoPredicate<>());
        assertThat(rating, equalTo(DynamodbQuerySelectionRater.RATING_NO_SELECTIVITY));
    }

    @Test
    void testEquality() {
        final int rating = new DynamodbQuerySelectionRater().rate(EQUAL);
        assertThat(rating, equalTo(DynamodbQuerySelectionRater.RATING_EQUALITY));
    }

    @Test
    void testAndWithTwoEqualities() {
        final int rating = new DynamodbQuerySelectionRater()
                .rate(new LogicalOperator<>(List.of(EQUAL, EQUAL), LogicalOperator.Operator.AND));
        assertThat(rating, equalTo(DynamodbQuerySelectionRater.RATING_EQUALITY));
    }

    @Test
    void testAndWithOneEqualityAndOneNoPredicate() {
        final int rating = new DynamodbQuerySelectionRater()
                .rate(new LogicalOperator<>(List.of(EQUAL, new NoPredicate<>()), LogicalOperator.Operator.AND));
        assertThat(rating, equalTo(DynamodbQuerySelectionRater.RATING_EQUALITY));
    }

    @Test
    void testOrWithTwoEqualities() {
        final int rating = new DynamodbQuerySelectionRater()
                .rate(new LogicalOperator<>(List.of(EQUAL, EQUAL), LogicalOperator.Operator.OR));
        assertThat(rating, equalTo(DynamodbQuerySelectionRater.RATING_EQUALITY));
    }

    @Test
    void testOrWithOneEqualityAndOneNoPredicate() {
        final int rating = new DynamodbQuerySelectionRater()
                .rate(new LogicalOperator<>(List.of(EQUAL, new NoPredicate<>()), LogicalOperator.Operator.OR));
        assertThat(rating, equalTo(DynamodbQuerySelectionRater.RATING_NO_SELECTIVITY));
    }
}