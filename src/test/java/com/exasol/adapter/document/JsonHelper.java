package com.exasol.adapter.document;

import java.io.IOException;
import java.io.StringWriter;

import jakarta.json.*;

public class JsonHelper {

    private JsonHelper() {
        // static class
    }

    public static String toJson(final JsonObject jsonConfig) {
        try (final StringWriter writer = new StringWriter()) {
            try (final JsonWriter jsonWriter = Json.createWriter(writer)) {
                jsonWriter.write(jsonConfig);
            }
            return writer.toString();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
