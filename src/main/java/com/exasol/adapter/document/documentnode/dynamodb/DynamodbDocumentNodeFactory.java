package com.exasol.adapter.document.documentnode.dynamodb;

import java.math.BigDecimal;

import com.exasol.adapter.document.documentnode.DocumentNode;
import com.exasol.adapter.document.documentnode.holder.*;
import com.exasol.errorreporting.ExaError;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * This class builds document nodes for a given {@link AttributeValue}. Whether an object, an array, or a value-node is
 * built depends on the type of the {@link AttributeValue}.
 */
public class DynamodbDocumentNodeFactory {

    /**
     * Builds a document node for a given {@link AttributeValue}.
     *
     * @param attributeValue {@link AttributeValue} to wrap
     * @return object, array, or value-node
     */
    public DocumentNode buildDocumentNode(final AttributeValue attributeValue) {
        if (attributeValue.nul() != null && attributeValue.nul()) {
            return new NullHolderNode();
        } else if (attributeValue.s() != null) {
            return new StringHolderNode(attributeValue.s());
        } else if (attributeValue.n() != null) {
            return new BigDecimalHolderNode(new BigDecimal(attributeValue.n()));
        } else if (attributeValue.b() != null) {
            return new BinaryHolderNode(attributeValue.b().asByteArray());
        } else if (attributeValue.bool() != null) {
            return new BooleanHolderNode(attributeValue.bool());
        } else if (attributeValue.hasM()) {
            return new DynamodbMap(attributeValue.m());
        } else if (attributeValue.hasBs()) {
            return new DynamodbBinarySet(attributeValue.bs());
        } else if (attributeValue.hasL()) {
            return new DynamodbList(attributeValue.l());
        } else if (attributeValue.hasNs()) {
            return new DynamodbNumberSet(attributeValue.ns());
        } else if (attributeValue.hasSs()) {
            return new DynamodbStringSet(attributeValue.ss());
        } else {
            throw new UnsupportedOperationException(ExaError.messageBuilder("F-VS-DY-7").message(
                    "The type of the DynamoDB node {{node}} is not supported in this version of the dynamodb-virtual-schema.",
                    attributeValue).ticketMitigation().toString());
        }
    }
}
