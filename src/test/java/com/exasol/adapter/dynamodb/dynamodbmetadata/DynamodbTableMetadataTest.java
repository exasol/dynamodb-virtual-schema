package com.exasol.adapter.dynamodb.dynamodbmetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.exasol.utils.StringSerializer;

class DynamodbTableMetadataTest {
    private static final DynamodbKey PRIMARY_KEY = new DynamodbKey(null, Optional.empty());
    private static final DynamodbKey GLOBAL_INDEX = new DynamodbKey(null, Optional.empty());
    private static final DynamodbKey LOCAL_INDEX = new DynamodbKey(null, Optional.empty());
    private static final DynamodbTableMetadata TABLE_METADATA = new DynamodbTableMetadata(PRIMARY_KEY, List.of(LOCAL_INDEX), List.of(GLOBAL_INDEX));


    @Test
    void testGetPrimaryKey() {
        assertThat(TABLE_METADATA.getPrimaryKey(), equalTo(PRIMARY_KEY));
    }

    @Test
    void tesGetLocalIndexes() {
        assertThat(TABLE_METADATA.getLocalIndexes(), containsInAnyOrder(LOCAL_INDEX));
    }

    @Test
    void tesGetGlobalIndexes() {
        assertThat(TABLE_METADATA.getGlobalIndexes(), containsInAnyOrder(GLOBAL_INDEX));
    }

    @Test
    void tesGetAllIndexes() {
        assertThat(TABLE_METADATA.getAllIndexes(), containsInAnyOrder(GLOBAL_INDEX, LOCAL_INDEX));
    }

    @Test
    void testSerialization() throws IOException, ClassNotFoundException {
        final String serialized = StringSerializer.serializeToString(TABLE_METADATA);
        final DynamodbTableMetadata result = (DynamodbTableMetadata) StringSerializer.deserializeFromString(serialized);
        assertThat(result.getPrimaryKey(), not(equalTo(null)));
    }
}