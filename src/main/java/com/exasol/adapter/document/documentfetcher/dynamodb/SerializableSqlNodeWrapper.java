package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.io.*;
import java.util.Collections;

import javax.json.*;

import com.exasol.adapter.request.parser.PushdownSqlParser;
import com.exasol.adapter.request.renderer.PushdownSqlRenderer;
import com.exasol.adapter.sql.SqlNode;

import software.amazon.awssdk.utils.StringInputStream;

/**
 * This class wraps a {@link SqlNode} to make it serializable.
 */
public class SerializableSqlNodeWrapper implements Serializable {
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

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.writeUTF(new PushdownSqlRenderer().render(this.sqlNode).toString());
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        try (final StringInputStream jsonInputStream = new StringInputStream(in.readUTF());
                final JsonReader jsonReader = Json.createReader(jsonInputStream)) {
            final JsonObject sqlNodeAsJson = jsonReader.readObject();
            this.sqlNode = PushdownSqlParser.createWithTablesMetadata(Collections.emptyList())
                    .parseExpression(sqlNodeAsJson);
        }
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("Stream data required");
    }
}
