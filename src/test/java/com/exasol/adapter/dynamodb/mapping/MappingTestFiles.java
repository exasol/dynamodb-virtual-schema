package com.exasol.adapter.dynamodb.mapping;

import java.io.File;

public class MappingTestFiles {
    public static final String BASIC_MAPPING_FILE_NAME = "basicMapping.json";
    public static final File BASIC_MAPPING_FILE = readFile(BASIC_MAPPING_FILE_NAME);

    public static final String TO_JSON_MAPPING_FILE_NAME = "toJsonMapping.json";
    public static final File TO_JSON_MAPPING_FILE = readFile(TO_JSON_MAPPING_FILE_NAME);

    public static final String INVALID_TO_STRING_MAPPING_AT_ROOT_LEVEL_FILE_NAME = "invalidToStringMappingAtRootLevel.json";
    public static final File INVALID_TO_STRING_MAPPING_AT_ROOT_LEVEL_FILE = readFile(
            INVALID_TO_STRING_MAPPING_AT_ROOT_LEVEL_FILE_NAME);

    private static File readFile(final String fileName) {
        return new File(MappingTestFiles.class.getClassLoader().getResource(fileName).getFile());
    }
}
