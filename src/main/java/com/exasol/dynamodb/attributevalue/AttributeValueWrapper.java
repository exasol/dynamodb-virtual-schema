package com.exasol.dynamodb.attributevalue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Wrapper for {@link com.amazonaws.services.dynamodbv2.model.AttributeValue} that accepts {@link AttributeValueVisitor}.
 */
public class AttributeValueWrapper {
    private final AttributeValue attributeValue;

    /**
     * Creates an instance of {@link AttributeValueWrapper}.
     *
     * @param attributeValue DynamoDB value to wrap
     */
    public AttributeValueWrapper(final AttributeValue attributeValue) {
        this.attributeValue = attributeValue;
    }

    /**
     * Accepts the {@link AttributeValueVisitor}.
     * 
     * @param visitor {@link AttributeValueVisitor}
     */
    public void accept(final AttributeValueVisitor visitor) {
        if (this.attributeValue.getNULL() != null && this.attributeValue.getNULL()) {
            visitor.visitNull();
        } else if (this.attributeValue.getS() != null) {
            visitor.visitString(this.attributeValue.getS());
        } else if (this.attributeValue.getN() != null) {
            visitor.visitNumber(this.attributeValue.getN());
        } else if (this.attributeValue.getB() != null) {
            visitor.visitBinary(this.attributeValue.getB());
        } else if (this.attributeValue.getBOOL() != null) {
            visitor.visitBoolean(this.attributeValue.getBOOL());
        } else if (this.attributeValue.getM() != null) {
            visitor.visitMap(this.attributeValue.getM());
        } else if (this.attributeValue.getBS() != null) {
            visitor.visitByteSet(this.attributeValue.getBS());
        } else if (this.attributeValue.getL() != null) {
            visitor.visitList(this.attributeValue.getL());
        } else if (this.attributeValue.getNS() != null) {
            visitor.visitNumberSet(this.attributeValue.getNS());
        } else if (this.attributeValue.getSS() != null) {
            visitor.visitStringSet(this.attributeValue.getSS());
        } else {
            throw new UnsupportedOperationException("Unsupported DynamoDB type");
        }
    }
}
