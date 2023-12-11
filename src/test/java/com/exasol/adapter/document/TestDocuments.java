package com.exasol.adapter.document;

import java.io.InputStream;
import java.util.Objects;

public class TestDocuments {
    public static final String BOOKS_ISBN_PROPERTY = "isbn";
    public static final String DATA_TYPE_TEST_STRING_VALUE = "stringValue";

    public static InputStream books() {
        return getResource("books.json");
    }

    public static InputStream dataTypeTest() {
        return getResource("dataTypeTest.json");
    }

    private static InputStream getResource(final String resourceName) {
        return Objects.requireNonNull(TestDocuments.class.getClassLoader().getResourceAsStream(resourceName),
                "Resource '" + resourceName + "' not found");
    }
}
