package com.exasol.adapter.dynamodb.mapping;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.json.JSONObject;
import org.json.JSONTokener;

public class MappingTestFiles {
    public static final String BASIC_MAPPING_FILE_NAME = "basicMapping.json";
    public static final File BASIC_MAPPING_FILE = readFile(BASIC_MAPPING_FILE_NAME);

    public static final String TO_JSON_MAPPING_FILE_NAME = "toJsonMapping.json";
    public static final File TO_JSON_MAPPING_FILE = readFile(TO_JSON_MAPPING_FILE_NAME);

    public static final String SINGLE_COLUMN_TO_TABLE_MAPPING_FILE_NAME = "singleColumnToTableMapping.json";
    public static final File SINGLE_COLUMN_TO_TABLE_MAPPING_FILE = readFile(SINGLE_COLUMN_TO_TABLE_MAPPING_FILE_NAME);

    public static final String MULTI_COLUMN_TO_TABLE_MAPPING_FILE_NAME = "multiColumnToTableMapping.json";
    public static final File MULTI_COLUMN_TO_TABLE_MAPPING_FILE = readFile(MULTI_COLUMN_TO_TABLE_MAPPING_FILE_NAME);

    public static final String WHOLE_TABLE_TO_TABLE_MAPPING_FILE_NAME = "wholeTableToJsonMapping.json";
    public static final File WHOLE_TABLE_TO_TABLE_MAPPING_FILE = readFile(WHOLE_TABLE_TO_TABLE_MAPPING_FILE_NAME);

    public static final String DOUBLE_NESTED_TO_TABLE_MAPPING_FILE_NAME = "doubleNestedToTableMapping.json";
    public static final File DOUBLE_NESTED_TO_TABLE_MAPPING_FILE = readFile(DOUBLE_NESTED_TO_TABLE_MAPPING_FILE_NAME);

    public static final String OPEN_LIBRARY_MAPPING_FILE_NAME = "openLibraryMapping.json";

    public static final String DATA_TYPE_TEST_MAPPING_FILE_NAME = "dataTypeTestMapping.json";
    public static final String DATA_TYPE_TEST_SRC_TABLE_NAME = "DATA_TYPE_TEST";
    public static final String DATA_TYPE_TEST_EXASOL_TABLE_NAME = "TEST";

    private final List<File> tempFiles = new ArrayList<>();

    private static File readFile(final String fileName) {
        return new File(MappingTestFiles.class.getClassLoader().getResource(fileName).getFile());
    }

    /**
     * This method generates a invalid file from a valid using an invalidator function. It uses the org.json api because
     * it provides modifiable objects in contrast to the projects default javax.json api.
     *
     * @param base        Definition to use as basis
     * @param invalidator Function that modifies / invalidates the definition.
     * @return File containing modified definition.
     * @throws IOException on read or write error.
     */
    public File generateInvalidFile(final File base, final Function<JSONObject, JSONObject> invalidator)
            throws IOException {
        final File tempFile = File.createTempFile("schemaTmp", ".json");
        this.tempFiles.add(tempFile);
        try (final InputStream inputStream = new FileInputStream(base);
                final FileWriter fileWriter = new FileWriter(tempFile)) {
            final JSONObject baseObject = new JSONObject(new JSONTokener(inputStream));
            final JSONObject invalidObject = invalidator.apply(baseObject);
            fileWriter.write(invalidObject.toString());
            fileWriter.close();
            return tempFile;
        }
    }

    public void deleteAllTempFiles() {
        for (final File tempFile : this.tempFiles) {
            tempFile.delete();
        }
        this.tempFiles.clear();
    }
}
