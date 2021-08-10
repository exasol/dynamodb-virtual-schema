package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentStringValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbListTest {
    private static final AttributeValue NESTED_1 = AttributeValueQuickCreator.forString("test1");
    private static final AttributeValue NESTED_2 = AttributeValueQuickCreator.forString("test2");
    private static final AttributeValue LIST = AttributeValueQuickCreator.forList(NESTED_1, NESTED_2);

    @Test
    void testGetValue() {
        final DynamodbList dynamodbList = (DynamodbList) new DynamodbDocumentNodeFactory().buildDocumentNode(LIST);
        final DocumentStringValue result = (DocumentStringValue) dynamodbList.getValue(0);
        assertThat(result.getValue(), equalTo(NESTED_1.s()));
    }

    @Test
    void testGetValues() {
        final DynamodbList dynamodbList = (DynamodbList) new DynamodbDocumentNodeFactory().buildDocumentNode(LIST);
        final DocumentStringValue result1 = (DocumentStringValue) dynamodbList.getValuesList().get(0);
        final DocumentStringValue result2 = (DocumentStringValue) dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getValue(), result2.getValue()),
                containsInRelativeOrder(NESTED_1.s(), NESTED_2.s()));
    }
}
