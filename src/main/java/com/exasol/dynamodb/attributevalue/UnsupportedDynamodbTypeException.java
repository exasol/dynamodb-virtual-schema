package com.exasol.dynamodb.attributevalue;

/**
 * Exception that is thrown by {@link AttributeValueVisitor} if method was not implemented.
 */
public class UnsupportedDynamodbTypeException extends UnsupportedOperationException {
    private final String dynamodbTypeName;

    /**
     * Creates an instance of {@link UnsupportedDynamodbTypeException}.
     *
     * @param dynamodbTypeName name of the unimplemented DynamoDB type
     */
    UnsupportedDynamodbTypeException(final String dynamodbTypeName) {
        super("Unsupported DynamoDB type: " + dynamodbTypeName);
        this.dynamodbTypeName = dynamodbTypeName;
    }

    /**
     * Getter for the name of the unsupported DynamoDB type.
     *
     * @return unsupported DynamoDB type
     */
    public String getDynamodbTypeName() {
        return this.dynamodbTypeName;
    }
}
