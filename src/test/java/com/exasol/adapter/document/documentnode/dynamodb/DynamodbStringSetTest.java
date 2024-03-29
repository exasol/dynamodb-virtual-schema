package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentStringValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbStringSetTest {
    private static final String STRING_1 = "string1";
    private static final String STRING_2 = "string2";
    private static final AttributeValue STRING_SET = AttributeValueQuickCreator
            .forStringSet(List.of(STRING_1, STRING_2));

    @Test
    void testGetValue() {
        final DynamodbStringSet dynamodbList = (DynamodbStringSet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(STRING_SET);
        final DocumentStringValue result = dynamodbList.getValue(0);
        assertThat(result.getValue(), equalTo(STRING_1));
    }

    @Test
    void testGetValues() {
        final DynamodbStringSet dynamodbList = (DynamodbStringSet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(STRING_SET);
        final DocumentStringValue result1 = dynamodbList.getValuesList().get(0);
        final DocumentStringValue result2 = dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getValue(), result2.getValue()), containsInAnyOrder(STRING_1, STRING_2));
    }
}
