package com.exasol.adapter.document.mapping;

import java.io.File;

public class TestDocuments {
    public static final File BOOKS = new File(TestDocuments.class.getClassLoader().getResource("books.json").getFile());
    public static final String BOOKS_ISBN_PROPERTY = "isbn";
    public static final File DATA_TYPE_TEST = new File(
            TestDocuments.class.getClassLoader().getResource("dataTypeTest.json").getFile());
    public static final String DATA_TYPE_TEST_STRING_VALUE = "stringValue";
}
