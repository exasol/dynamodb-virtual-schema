package com.exasol.adapter.dynamodb.mapping;

import java.io.File;

public class TestDocuments {
    public static final File BOOKS = new File(TestDocuments.class.getClassLoader().getResource("books.json").getFile());
    public static final String BOOKS_ISBN_PROPERTY = "isbn";
}
