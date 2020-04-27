package com.exasol.adapter.dynamodb.queryrunner;

import static com.exasol.adapter.dynamodb.queryrunner.TestSetup.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbKey;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.remotetablequery.*;

public class DynamodbGetItemQueryPlanFactoryTest {
    final TestSetup testSetup;

    public DynamodbGetItemQueryPlanFactoryTest() throws IOException {
        this.testSetup = new TestSetup();
    }

    @Test
    void testSimplePrimaryKey() throws IOException, PlanDoesNotFitException {
        final String filter = "test";
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> selection = new ColumnLiteralComparisonPredicate<>(
                ComparisonPredicate.Operator.EQUAL, COLUMN1_MAPPING, new DynamodbString(filter));
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = new RemoteTableQuery<>(TABLE_MAPPING,
                TABLE_MAPPING.getColumns(), selection);

        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), Collections.emptyList(), Collections.emptyList());
        final DynamodbGetItemQueryPlan getItemPlan = new DynamodbGetItemQueryPlanFactory()
                .buildGetItemPlanIfPossible(documentQuery, dynamodbTableMetadata);
        final Map<String, AttributeValue> key = getItemPlan.getGetItemRequest().getKey();
        assertThat(getItemPlan.getGetItemRequest().getTableName(), equalTo(TABLE_NAME));
        assertThat(key.size(), equalTo(1));
        assertThat(key.get(COLUMN1_NAME).getS(), equalTo(filter));
    }

    @Test
    void testSimplePrimaryKeyWithSecondNonKeySelection() throws IOException, PlanDoesNotFitException {
        final String filter = "test";
        final QueryPredicate<DynamodbNodeVisitor> selection = new BinaryLogicalOperator<>(List.of(
                new ColumnLiteralComparisonPredicate<>(ComparisonPredicate.Operator.EQUAL, COLUMN1_MAPPING,
                        new DynamodbString(filter)),
                new ColumnLiteralComparisonPredicate<>(ComparisonPredicate.Operator.EQUAL, COLUMN2_MAPPING,
                        new DynamodbString("test2"))),
                BinaryLogicalOperator.Operator.AND);
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = new RemoteTableQuery<>(TABLE_MAPPING,
                TABLE_MAPPING.getColumns(), selection);

        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), Collections.emptyList(), Collections.emptyList());
        final DynamodbGetItemQueryPlan getItemPlan = new DynamodbGetItemQueryPlanFactory()
                .buildGetItemPlanIfPossible(documentQuery, dynamodbTableMetadata);
        final Map<String, AttributeValue> key = getItemPlan.getGetItemRequest().getKey();
        assertThat(getItemPlan.getGetItemRequest().getTableName(), equalTo(TABLE_NAME));
        assertThat(key.size(), equalTo(1));
        assertThat(key.get(COLUMN1_NAME).getS(), equalTo(filter));
    }

    @Test
    void testCompoundPrimaryKey() throws IOException, PlanDoesNotFitException {
        final String filter1 = "test";
        final String filter2 = "test2";
        final QueryPredicate<DynamodbNodeVisitor> selection = new BinaryLogicalOperator<>(List.of(
                new ColumnLiteralComparisonPredicate<>(ComparisonPredicate.Operator.EQUAL, COLUMN1_MAPPING,
                        new DynamodbString(filter1)),
                new ColumnLiteralComparisonPredicate<>(ComparisonPredicate.Operator.EQUAL, COLUMN2_MAPPING,
                        new DynamodbString(filter2))),
                BinaryLogicalOperator.Operator.AND);
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = new RemoteTableQuery<>(TABLE_MAPPING,
                TABLE_MAPPING.getColumns(), selection);

        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.of(COLUMN2_NAME)), Collections.emptyList(),
                Collections.emptyList());
        final DynamodbGetItemQueryPlan getItemPlan = new DynamodbGetItemQueryPlanFactory()
                .buildGetItemPlanIfPossible(documentQuery, dynamodbTableMetadata);
        final Map<String, AttributeValue> key = getItemPlan.getGetItemRequest().getKey();
        assertThat(getItemPlan.getGetItemRequest().getTableName(), equalTo(TABLE_NAME));
        assertThat(key.size(), equalTo(2));
        assertThat(key.get(COLUMN1_NAME).getS(), equalTo(filter1));
        assertThat(key.get(COLUMN2_NAME).getS(), equalTo(filter2));
    }

    @Test
    void testNoSelection() throws IOException, PlanDoesNotFitException {
        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.empty()), Collections.emptyList(), Collections.emptyList());

        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = new RemoteTableQuery<>(TABLE_MAPPING,
                TABLE_MAPPING.getColumns(), new NoPredicate<>());
        final PlanDoesNotFitException exception = assertThrows(PlanDoesNotFitException.class,
                () -> new DynamodbGetItemQueryPlanFactory().buildGetItemPlanIfPossible(documentQuery,
                        dynamodbTableMetadata));
        assertThat(exception.getMessage(),
                equalTo("Not a GetItem request as the partition key was not specified in the where clause."));
    }

    @Test
    void testSelectionWithOr() throws IOException, PlanDoesNotFitException {
        final String filter1 = "test";
        final String filter2 = "test2";
        final QueryPredicate<DynamodbNodeVisitor> selection = new BinaryLogicalOperator<>(
                List.of(new NoPredicate<>(), new NoPredicate<>()), BinaryLogicalOperator.Operator.OR);
        final RemoteTableQuery<DynamodbNodeVisitor> documentQuery = new RemoteTableQuery<>(TABLE_MAPPING,
                TABLE_MAPPING.getColumns(), selection);

        final DynamodbTableMetadata dynamodbTableMetadata = new DynamodbTableMetadata(
                new DynamodbKey(COLUMN1_NAME, Optional.of(COLUMN2_NAME)), Collections.emptyList(),
                Collections.emptyList());
        final PlanDoesNotFitException exception = assertThrows(PlanDoesNotFitException.class,
                () -> new DynamodbGetItemQueryPlanFactory().buildGetItemPlanIfPossible(documentQuery,
                        dynamodbTableMetadata));
        assertThat(exception.getMessage(), equalTo("OR operators are not supported for GetItem requests."));
    }
}
