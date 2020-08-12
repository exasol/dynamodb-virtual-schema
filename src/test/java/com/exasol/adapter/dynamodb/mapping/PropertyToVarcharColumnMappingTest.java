package com.exasol.adapter.dynamodb.mapping;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

class PropertyToVarcharColumnMappingTest {
    private static final String COLUMN_NAME = "columnName";
    private static final int STRING_LENGTH = 10;
    private static final PropertyToVarcharColumnMapping TEST_OBJECT = new PropertyToVarcharColumnMapping(COLUMN_NAME,
            DocumentPathExpression.builder().addArrayAll().build(), MappingErrorBehaviour.NULL, STRING_LENGTH,
            TruncateableMappingErrorBehaviour.TRUNCATE);

    @Test
    void testDestinationDataType() {
        assertAll(
                () -> assertThat(TEST_OBJECT.getExasolDataType().getExaDataType(),
                        equalTo(DataType.ExaDataType.VARCHAR)),
                () -> assertThat(TEST_OBJECT.getExasolDataType().getSize(), equalTo(STRING_LENGTH))//
        );
    }

    @Test
    void testIsDestinationNullable() {
        assertThat(TEST_OBJECT.isExasolColumnNullable(), equalTo(true));
    }

    @Test
    void testEqual() {
        final PropertyToVarcharColumnMapping other = new PropertyToVarcharColumnMapping(COLUMN_NAME,
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getMappingErrorBehaviour(),
                TEST_OBJECT.getVarcharColumnSize(), TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentName() {
        final PropertyToVarcharColumnMapping other = new PropertyToVarcharColumnMapping("otherName",
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getMappingErrorBehaviour(),
                TEST_OBJECT.getVarcharColumnSize(), TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentPath() {
        final PropertyToVarcharColumnMapping other = new PropertyToVarcharColumnMapping(COLUMN_NAME,
                DocumentPathExpression.empty(), TEST_OBJECT.getMappingErrorBehaviour(),
                TEST_OBJECT.getVarcharColumnSize(),
                TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentStringSize() {
        final PropertyToVarcharColumnMapping other = new PropertyToVarcharColumnMapping(COLUMN_NAME,
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getMappingErrorBehaviour(), 123,
                TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }
}
