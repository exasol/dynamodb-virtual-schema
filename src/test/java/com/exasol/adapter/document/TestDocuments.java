package com.exasol.adapter.document;

import java.io.InputStream;

public class TestDocuments {
    public static final String BOOKS_ISBN_PROPERTY = "isbn";
    public static final String DATA_TYPE_TEST_STRING_VALUE = "stringValue";

    public static InputStream books() {
        return TestDocuments.class.getClassLoader().getResourceAsStream("books.json");
    }

    public static InputStream dataTypeTest() {
        return TestDocuments.class.getClassLoader().getResourceAsStream("dataTypeTest.json");
    }
}
