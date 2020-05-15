package com.exasol.adapter.dynamodb.mapping;

import static com.exasol.EqualityTest.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityTest.assertSymmetricNotEqualWithHashAndEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.metadata.DataType;

public class ToStringPropertyToColumnMappingTest {
    private static final String COLUMN_NAME = "columnName";
    private static final int STRING_LENGTH = 10;
    private static final ToStringPropertyToColumnMapping TEST_OBJECT = new ToStringPropertyToColumnMapping(COLUMN_NAME,
            new DocumentPathExpression.Builder().addArrayAll().build(), LookupFailBehaviour.DEFAULT_VALUE,
            STRING_LENGTH, ToStringPropertyToColumnMapping.OverflowBehaviour.TRUNCATE);

    @Test
    void testDestinationDataType() {
        assertAll(
                () -> assertThat(TEST_OBJECT.getExasolDataType().getExaDataType(),
                        equalTo(DataType.ExaDataType.VARCHAR)),
                () -> assertThat(TEST_OBJECT.getExasolDataType().getSize(), equalTo(STRING_LENGTH))//
        );
    }

    @Test
    void testGetDestinationDefaultValue() {
        assertThat(TEST_OBJECT.getExasolDefaultValue().toString(), equalTo(""));
    }

    @Test
    void testIsDestinationNullable() {
        assertThat(TEST_OBJECT.isExasolColumnNullable(), equalTo(true));
    }

    @Test
    void testEqual() {
        final ToStringPropertyToColumnMapping other = new ToStringPropertyToColumnMapping(COLUMN_NAME,
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour(),
                TEST_OBJECT.getExasolStringSize(), TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentName() {
        final ToStringPropertyToColumnMapping other = new ToStringPropertyToColumnMapping("otherName",
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour(),
                TEST_OBJECT.getExasolStringSize(), TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentPath() {
        final ToStringPropertyToColumnMapping other = new ToStringPropertyToColumnMapping(COLUMN_NAME,
                DocumentPathExpression.empty(), TEST_OBJECT.getLookupFailBehaviour(), TEST_OBJECT.getExasolStringSize(),
                TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentStringSize() {
        final ToStringPropertyToColumnMapping other = new ToStringPropertyToColumnMapping(COLUMN_NAME,
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour(), 123,
                TEST_OBJECT.getOverflowBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }
}
