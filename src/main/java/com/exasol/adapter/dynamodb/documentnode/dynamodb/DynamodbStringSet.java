package com.exasol.adapter.dynamodb.documentnode.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.documentnode.DocumentArray;
import com.exasol.adapter.dynamodb.documentnode.DocumentNode;

/**
 * This class wraps {@link com.amazonaws.services.dynamodbv2.model.AttributeValue}s with a String set value
 */
public class DynamodbStringSet implements DocumentArray {
    private final Collection<String> value;

    private AttributeValue attributeValueFor(final String string) {
        final AttributeValue attributeValue = new AttributeValue();
        attributeValue.setS(string);
        return attributeValue;
    }

    DynamodbStringSet(final AttributeValue valueToWrap) {
        this.value = valueToWrap.getSS();
    }

    @Override
    public List<DocumentNode> getValueList() {
        final DynamodbDocumentNodeFactory dynamodbDocumentNodeFactory = new DynamodbDocumentNodeFactory();
        return this.value.stream()
                .map(stringValue -> dynamodbDocumentNodeFactory.buildDocumentNode(attributeValueFor(stringValue)))
                .collect(Collectors.toList());
    }

    @Override
    public DocumentNode getValue(final int index) {
        final String[] valueArray = this.value.toArray(new String[0]);
        return new DynamodbDocumentNodeFactory().buildDocumentNode(attributeValueFor(valueArray[index]));
    }
}
