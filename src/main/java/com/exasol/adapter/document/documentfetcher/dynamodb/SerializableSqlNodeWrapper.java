package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.io.*;
import java.util.Collections;

import com.exasol.adapter.request.parser.PushdownSqlParser;
import com.exasol.adapter.request.renderer.PushdownSqlRenderer;
import com.exasol.adapter.sql.SqlNode;

import jakarta.json.*;
import software.amazon.awssdk.utils.StringInputStream;

/**
 * This class wraps a {@link SqlNode} to make it serializable.
 */
public class SerializableSqlNodeWrapper implements Serializable {
    private static final long serialVersionUID = 1L;
    private SqlNode sqlNode;

    /**
     * Create a new instance of {@link SerializableSqlNodeWrapper}.
     *
     * @param sqlNode {@link SqlNode} to wrap
     */
    public SerializableSqlNodeWrapper(final SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }

    /**
     * Get the wrapped {@link SqlNode}.
     *
     * @return wrapped {@link SqlNode}
     */
    public SqlNode getSqlNode() {
        return this.sqlNode;
    }

    /**
     * Custom serialization.
     * <p>
     * This method is called during deserialization
     * </p>
     *
     * @param out stream to write to
     * @throws IOException if serialization fails
     */
    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.writeUTF(new PushdownSqlRenderer().render(this.sqlNode).toString());
    }

    /**
     * Custom deserialization.
     * <p>
     * This method is called during deserialization
     * </p>
     */
    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        try (final StringInputStream jsonInputStream = new StringInputStream(in.readUTF());
                final JsonReader jsonReader = Json.createReader(jsonInputStream)) {
            final JsonObject sqlNodeAsJson = jsonReader.readObject();
            this.sqlNode = PushdownSqlParser.createWithTablesMetadata(Collections.emptyList())
                    .parseExpression(sqlNodeAsJson);
        }
    }

    /**
     * Custom deserialization.
     * <p>
     * This method is called during deserialization
     * </p>
     * 
     * @throws ObjectStreamException since no data is available if this method is called
     */
    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException(
                ExaError.messageBuilder("E-VS-DY-34").message("Failed to deserialize SqlNode.").toString());
    }
}
