package com.exasol.adapter.dynamodb.queryrunner;

import static com.exasol.adapter.dynamodb.queryrunner.TestSetup.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbKey;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.sql.*;

public class DynamodbGetItemQueryPlanFactoryTest {
    final TestSetup testSetup;

    public DynamodbGetItemQueryPlanFactoryTest() throws IOException {
        this.testSetup = new TestSetup();
    }

    @Test
    void testSimplePrimaryKey() throws IOException, PlanDoesNotFitException {
        final String filter = "test";
        final SqlStatement selectStatement = this.testSetup.getSelectWithWhereClause(
                new SqlPredicateEqual(new SqlLiteralString(filter), new SqlColumn(0, this.testSetup.column1Metadata)));
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), List.of(), List.of());
        final DynamodbGetItemQueryPlan getItemPlan = new DynamodbGetItemQueryPlanFactory()
                .buildGetItemPlanIfPossible(TABLE_NAME, selectStatement, dynamodbTableMetadata);
        final Map<String, AttributeValue> key = getItemPlan.getGetItemRequest().getKey();
        assertThat(getItemPlan.getGetItemRequest().getTableName(), equalTo(TABLE_NAME));
        assertThat(key.size(), equalTo(1));
        assertThat(key.get(COLUMN1_NAME).getS(), equalTo(filter));
    }

    @Test
    void testSimplePrimaryKeyWithSecondNonKeySelection() throws IOException, PlanDoesNotFitException {
        final String filter = "test";
        final SqlPredicateEqual indexPredicate = new SqlPredicateEqual(new SqlLiteralString(filter),
                new SqlColumn(0, this.testSetup.column1Metadata));
        final SqlPredicateEqual otherPredicate = new SqlPredicateEqual(new SqlLiteralString("test2"),
                new SqlColumn(1, this.testSetup.column2Metadata));
        final SqlStatement selectStatement = this.testSetup
                .getSelectWithWhereClause(new SqlPredicateAnd(List.of(indexPredicate, otherPredicate)));
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), List.of(), List.of());
        final DynamodbGetItemQueryPlan getItemPlan = new DynamodbGetItemQueryPlanFactory()
                .buildGetItemPlanIfPossible(TABLE_NAME, selectStatement, dynamodbTableMetadata);
        final Map<String, AttributeValue> key = getItemPlan.getGetItemRequest().getKey();
        assertThat(getItemPlan.getGetItemRequest().getTableName(), equalTo(TABLE_NAME));
        assertThat(key.size(), equalTo(1));
        assertThat(key.get(COLUMN1_NAME).getS(), equalTo(filter));
    }

    @Test
    void testCompoundPrimaryKey() throws IOException, PlanDoesNotFitException {
        final String filter1 = "test1";
        final String filter2 = "test2";
        final SqlPredicateEqual indexPredicate = new SqlPredicateEqual(new SqlLiteralString(filter1),
                new SqlColumn(0, this.testSetup.column1Metadata));
        final SqlPredicateEqual otherPredicate = new SqlPredicateEqual(new SqlLiteralString(filter2),
                new SqlColumn(1, this.testSetup.column2Metadata));
        final SqlStatement selectStatement = this.testSetup
                .getSelectWithWhereClause(new SqlPredicateAnd(List.of(indexPredicate, otherPredicate)));
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.of(COLUMN2_NAME)), List.of(), List.of());
        final DynamodbGetItemQueryPlan getItemPlan = new DynamodbGetItemQueryPlanFactory()
                .buildGetItemPlanIfPossible(TABLE_NAME, selectStatement, dynamodbTableMetadata);
        final Map<String, AttributeValue> key = getItemPlan.getGetItemRequest().getKey();
        assertThat(getItemPlan.getGetItemRequest().getTableName(), equalTo(TABLE_NAME));
        assertThat(key.size(), equalTo(2));
        assertThat(key.get(COLUMN1_NAME).getS(), equalTo(filter1));
        assertThat(key.get(COLUMN2_NAME).getS(), equalTo(filter2));
    }

    @Test
    void testNoSelection() throws IOException, PlanDoesNotFitException {
        final SqlStatement selectStatement = this.testSetup.getSelectWithWhereClause(null);
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), List.of(), List.of());
        final PlanDoesNotFitException exception = assertThrows(PlanDoesNotFitException.class,
                () -> new DynamodbGetItemQueryPlanFactory().buildGetItemPlanIfPossible(TABLE_NAME, selectStatement,
                        dynamodbTableMetadata));
        assertThat(exception.getMessage(),
                equalTo("This is not an getItem request as the query has no where clause and so no selection."));
    }

    @Test
    void testKeyWasNotSelected() throws IOException, PlanDoesNotFitException {
        final SqlStatement selectStatement = this.testSetup.getSelectWithWhereClause(new SqlPredicateAnd(List.of()));
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), List.of(), List.of());
        final PlanDoesNotFitException exception = assertThrows(PlanDoesNotFitException.class,
                () -> new DynamodbGetItemQueryPlanFactory().buildGetItemPlanIfPossible(TABLE_NAME, selectStatement,
                        dynamodbTableMetadata));
        assertThat(exception.getMessage(),
                equalTo("Not a GetItem request as the partition key was not specified in the where clause."));
    }

    @Test
    void testUnsupportedPredicate() throws IOException, PlanDoesNotFitException {
        final SqlStatement selectStatement = this.testSetup.getSelectWithWhereClause(new SqlPredicateOr(List.of()));
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), List.of(), List.of());
        final PlanDoesNotFitException exception = assertThrows(PlanDoesNotFitException.class,
                () -> new DynamodbGetItemQueryPlanFactory().buildGetItemPlanIfPossible(TABLE_NAME, selectStatement,
                        dynamodbTableMetadata));
        assertThat(exception.getMessage(), equalTo("This predicate is not supported for GetItem requests."));
    }
}
