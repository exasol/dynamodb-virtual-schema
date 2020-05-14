package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;
import com.exasol.sql.expression.IntegerLiteral;

class IterationIndexColumnMappingTest {
    private static final String EXASOL_COLUMN_NAME = "columnName";
    private static final DocumentPathExpression PATH = new DocumentPathExpression.Builder().addObjectLookup("test")
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
        assertAll(//
                () -> assertThat(TEST_OBJECT.equals(other), equalTo(true)),
                () -> assertThat(TEST_OBJECT.hashCode() == other.hashCode(), equalTo(true))//
        );
    }

    @Test
    void testNotEqualWithDifferentName() {
        final IterationIndexColumnMapping other = new IterationIndexColumnMapping("otherName", PATH);
        assertAll(//
                () -> assertThat(TEST_OBJECT.equals(other), equalTo(false)),
                () -> assertThat(TEST_OBJECT.hashCode() == other.hashCode(), equalTo(false))//
        );
    }

    @Test
    void testNotEqualWithDifferentPath() {
        final IterationIndexColumnMapping other = new IterationIndexColumnMapping(EXASOL_COLUMN_NAME,
                DocumentPathExpression.empty());
        assertAll(//
                () -> assertThat(TEST_OBJECT.equals(other), equalTo(false)),
                () -> assertThat(TEST_OBJECT.hashCode() == other.hashCode(), equalTo(false))//
        );
    }

    @Test
    void testWithNewExasolColumnNamePreservesAllProperties() {
        assertThat(TEST_OBJECT.withNewExasolName(EXASOL_COLUMN_NAME).equals(TEST_OBJECT), equalTo(true));
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
    void testExasolDefaultValue() {
        final IntegerLiteral defaultValue = (IntegerLiteral) TEST_OBJECT.getExasolDefaultValue();
        assertThat(defaultValue.getValue(), equalTo(-1));
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