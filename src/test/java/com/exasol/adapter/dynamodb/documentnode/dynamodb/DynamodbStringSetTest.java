package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

public class DynamodbStringSetTest {
    private static final String STRING_1 = "string1";
    private static final String STRING_2 = "string2";
    private static final AttributeValue STRING_SET = AttributeValueQuickCreator
            .forStringSet(List.of(STRING_1, STRING_2));

    @Test
    void testGetValue() {
        final DynamodbStringSet dynamodbList = (DynamodbStringSet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(STRING_SET);
        final DynamodbString result = (DynamodbString) dynamodbList.getValue(0);
        assertThat(result.getValue(), equalTo(STRING_1));
    }

    @Test
    void testGetValues() {
        final DynamodbStringSet dynamodbList = (DynamodbStringSet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(STRING_SET);
        final DynamodbString result1 = (DynamodbString) dynamodbList.getValuesList().get(0);
        final DynamodbString result2 = (DynamodbString) dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getValue(), result2.getValue()), containsInAnyOrder(STRING_1, STRING_2));
    }

    @Test
    void testVisitor() {
        final VisitationCheck visitor = new VisitationCheck();
        new DynamodbDocumentNodeFactory().buildDocumentNode(STRING_SET).accept(visitor);
        assertThat(visitor.wasVisited, equalTo(true));
    }

    private static class VisitationCheck implements DynamodbNodeVisitor {
        boolean wasVisited = false;

        @Override
        public void visit(final DynamodbStringSet value) {
            this.wasVisited = true;
        }

        @Override
        public void defaultVisit(final String typeName) {
            throw new IllegalStateException("Should not be called");
        }
    }
}
