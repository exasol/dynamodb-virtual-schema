package com.exasol.adapter.document.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

class AbstractPropertyToColumnMappingTest {
    private static final String EXASOL_COLUMN_NAME = "columnName";

    @Test
    void testColumnName() {
        final MockPropertyToColumnMapping columnMappingDefinition = new MockPropertyToColumnMapping(EXASOL_COLUMN_NAME,
                null, null);
        assertThat(columnMappingDefinition.getExasolColumnName(), equalTo(EXASOL_COLUMN_NAME));
    }
}
