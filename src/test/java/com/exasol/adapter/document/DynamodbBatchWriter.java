package com.exasol.adapter.document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.dynamodb.attributevalue.JsonToAttributeValueConverter;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/**
 * This class writes items from JSON strings to a DynamoDB table using the batchWrite operation.
 */
class DynamodbBatchWriter implements Consumer<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamodbBatchWriter.class);

    private static final int BATCH_SIZE = 20;
    final List<Map<String, AttributeValue>> batch = new ArrayList<>(BATCH_SIZE);
    private final DynamoDbClient dynamoClient;
    private final String tableName;
    private final long itemLimit;
    private long itemCounter;
    private long errorCounter = 0;

    public DynamodbBatchWriter(final DynamoDbClient dynamoClient, final String tableName) {
        this(dynamoClient, tableName, -1);
    }

    public DynamodbBatchWriter(final DynamoDbClient dynamoClient, final String tableName, final long itemLimit) {
        this.dynamoClient = dynamoClient;
        this.tableName = tableName;
        this.itemLimit = itemLimit;
    }

    @Override
    public void accept(final String line) {
        this.itemCounter++;
        if (this.itemLimit != -1 && this.itemCounter > this.itemLimit) {
            throw new LimitExceededException();
        }
        final Map<String, AttributeValue> item = JsonToAttributeValueConverter.getInstance().convert(line);
        this.batch.add(item);
        if (this.batch.size() >= BATCH_SIZE) {
            flush();
        }
    }

    public void flush() {
        if (this.batch.isEmpty()) {
            return;
        }
        final List<WriteRequest> writeRequests = this.batch.stream()
                .map(item -> PutRequest.builder().item(item).build())
                .map(putRequest -> WriteRequest.builder().putRequest(putRequest).build()).collect(Collectors.toList());
        final BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(Map.of(this.tableName, writeRequests)).build();
        try {
            this.dynamoClient.batchWriteItem(batchWriteItemRequest);
            LOGGER.info("# Written items: " + this.itemCounter);
        } catch (final DynamoDbException exception) {
            LOGGER.error(exception.getMessage());
            this.errorCounter++;
        }
        this.batch.clear();
    }

    public long getItemCounter() {
        return this.itemCounter;
    }

    public long getErrorCounter() {
        return this.errorCounter;
    }

    public static class LimitExceededException extends RuntimeException {
        private static final long serialVersionUID = -261552819884874212L;
    }
}
