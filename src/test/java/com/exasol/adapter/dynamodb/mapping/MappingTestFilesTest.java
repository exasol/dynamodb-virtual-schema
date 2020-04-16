package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

public class MappingTestFilesTest {

    @Test
    void testCreateAndDelete() throws IOException {
        final MappingTestFiles mappingTestFiles = new MappingTestFiles();
        final File invalidFile = mappingTestFiles.generateInvalidFile(MappingTestFiles.BASIC_MAPPING_FILE,
                base -> base);
        assertThat(invalidFile.exists(), equalTo(true));
        mappingTestFiles.deleteAllTempFiles();
        assertThat(invalidFile.exists(), equalTo(false));
    }
}
