package com.exasol.adapter.dynamodb.dynamodbmetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.utils.StringSerializer;

class DynamodbTableMetadataTest {
    DynamodbKey PRIMARY_KEY = new DynamodbKey(null, null);
    DynamodbKey GLOBAL_INDEX = new DynamodbKey(null, null);
    DynamodbKey LOCAL_INDEX = new DynamodbKey(null, null);

    private DynamodbTableMetadata getTableMetadata() {
        return new DynamodbTableMetadata(this.PRIMARY_KEY, List.of(this.LOCAL_INDEX), List.of(this.GLOBAL_INDEX));
    }

    @Test
    void testGetPrimaryKey() {
        final DynamodbTableMetadata tableMetadata = getTableMetadata();
        assertThat(tableMetadata.getPrimaryKey(), equalTo(this.PRIMARY_KEY));
    }

    @Test
    void tesGetLocalIndexes() {
        final DynamodbTableMetadata tableMetadata = getTableMetadata();
        assertThat(tableMetadata.getLocalIndexes(), containsInAnyOrder(this.LOCAL_INDEX));
    }

    @Test
    void tesGetGlobalIndexes() {
        final DynamodbTableMetadata tableMetadata = getTableMetadata();
        assertThat(tableMetadata.getGlobalIndexes(), containsInAnyOrder(this.GLOBAL_INDEX));
    }

    @Test
    void tesGetAllIndexes() {
        final DynamodbTableMetadata tableMetadata = getTableMetadata();
        assertThat(tableMetadata.getAllIndexes(), containsInAnyOrder(this.GLOBAL_INDEX, this.LOCAL_INDEX));
    }

    @Test
    void testSerialization() throws IOException, ClassNotFoundException {
        final DynamodbTableMetadata tableMetadata = getTableMetadata();
        final String serialized = StringSerializer.serializeToString(tableMetadata);
        final DynamodbTableMetadata result = (DynamodbTableMetadata) StringSerializer.deserializeFromString(serialized);
        assertThat(result.getPrimaryKey(), not(equalTo(null)));
    }
}