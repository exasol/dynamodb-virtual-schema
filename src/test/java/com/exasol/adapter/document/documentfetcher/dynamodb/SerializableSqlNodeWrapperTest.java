package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.*;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.sql.SqlLiteralString;

class SerializableSqlNodeWrapperTest {

    @Test
    void testSerialization() throws IOException, ClassNotFoundException {
        final SqlLiteralString sqlNode = new SqlLiteralString("test");
        final byte[] serialized = serialize(new SerializableSqlNodeWrapper(sqlNode));
        final SerializableSqlNodeWrapper deserializedWrapper = deserialize(serialized);
        final SqlLiteralString deserializedNode = (SqlLiteralString) deserializedWrapper.getSqlNode();
        assertThat(deserializedNode.getValue(), equalTo(sqlNode.getValue()));
    }

    private byte[] serialize(final SerializableSqlNodeWrapper sqlNode) throws IOException {
        try (final ByteArrayOutputStream targetStream = new ByteArrayOutputStream();
                final ObjectOutputStream serializationStream = new ObjectOutputStream(targetStream)) {
            serializationStream.writeObject(sqlNode);
            return targetStream.toByteArray();
        }
    }

    private SerializableSqlNodeWrapper deserialize(final byte[] serialized) throws IOException, ClassNotFoundException {
        try (final ByteArrayInputStream sourceStream = new ByteArrayInputStream(serialized);
                final ObjectInputStream deserializationStream = new ObjectInputStream(sourceStream)) {
            return (SerializableSqlNodeWrapper) deserializationStream.readObject();
        }
    }
}