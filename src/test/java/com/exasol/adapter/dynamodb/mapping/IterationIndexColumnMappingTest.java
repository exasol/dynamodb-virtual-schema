package com.exasol.adapter.dynamodb.mapping;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

class IterationIndexColumnMappingTest {
    private static final String EXASOL_COLUMN_NAME = "columnName";
    private static final DocumentPathExpression PATH = DocumentPathExpression.builder().addObjectLookup("test")
            .build();
    private static final IterationIndexColumnMapping TEST_OBJECT = new IterationIndexColumnMapping(EXASOL_COLUMN_NAME,
            PATH);

    @Test
    void testGetTablesPath() {
        assertThat(TEST_OBJECT.getTablesPath(), equalTo(PATH));
    }

    @Test
    void testEqual() {
        final IterationIndexColumnMapping other = new IterationIndexColumnMapping(EXASOL_COLUMN_NAME, PATH);
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentName() {
        final IterationIndexColumnMapping other = new IterationIndexColumnMapping("otherName", PATH);
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentPath() {
        final IterationIndexColumnMapping other = new IterationIndexColumnMapping(EXASOL_COLUMN_NAME,
                DocumentPathExpression.empty());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testWithNewExasolColumnNamePreservesAllProperties() {
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, TEST_OBJECT.withNewExasolName(EXASOL_COLUMN_NAME));
    }

    @Test
    void testWithNewExasolColumnNameChangesName() {
        final String newName = "newName";
        final ColumnMapping copy = TEST_OBJECT.withNewExasolName(newName);
        assertThat(copy.getExasolColumnName(), equalTo(newName));
    }

    @Test
    void testIsColumnNullable() {
        assertThat(TEST_OBJECT.isExasolColumnNullable(), equalTo(false));
    }

    @Test
    void testDataType() {
        final DataType exasolDataType = TEST_OBJECT.getExasolDataType();
        assertAll(//
                () -> assertThat(exasolDataType.getExaDataType(), equalTo(DataType.ExaDataType.DECIMAL)),
                () -> assertThat(exasolDataType.getPrecision(), equalTo(9)),
                () -> assertThat(exasolDataType.getScale(), equalTo(0))//
        );
    }
}