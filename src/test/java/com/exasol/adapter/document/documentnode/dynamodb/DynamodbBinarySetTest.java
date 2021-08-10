package com.exasol.adapter.document.documentnode.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.documentnode.DocumentBinaryValue;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamodbBinarySetTest {
    private static final SdkBytes BINARY_1 = SdkBytes.fromUtf8String("test1");
    private static final SdkBytes BINARY_2 = SdkBytes.fromUtf8String("test1");
    private static final AttributeValue BINARY_SET = AttributeValueQuickCreator
            .forBinarySet(List.of(BINARY_1, BINARY_2));

    @Test
    void testGetValue() {
        final DynamodbBinarySet dynamodbList = (DynamodbBinarySet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(BINARY_SET);
        final DocumentBinaryValue result = dynamodbList.getValue(0);
        assertThat(result.getBinary(), equalTo(BINARY_1.asByteArray()));
    }

    @Test
    void testGetValues() {
        final DynamodbBinarySet dynamodbList = (DynamodbBinarySet) new DynamodbDocumentNodeFactory()
                .buildDocumentNode(BINARY_SET);
        final DocumentBinaryValue result1 = dynamodbList.getValuesList().get(0);
        final DocumentBinaryValue result2 = dynamodbList.getValuesList().get(1);
        assertThat(List.of(result1.getBinary(), result2.getBinary()),
                containsInAnyOrder(BINARY_1.asByteArray(), BINARY_2.asByteArray()));
    }
}
