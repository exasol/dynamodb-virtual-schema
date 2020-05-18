package com.exasol.adapter.dynamodb.mapping;

import static com.exasol.EqualityTest.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityTest.assertSymmetricNotEqualWithHashAndEquals;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

class ToJsonPropertyToColumnMappingTest {

    private static final ToJsonPropertyToColumnMapping TEST_OBJECT = new ToJsonPropertyToColumnMapping("name",
            new DocumentPathExpression.Builder().addArrayAll().build(), LookupFailBehaviour.EXCEPTION);

    @Test
    void testEqual() {
        final ToJsonPropertyToColumnMapping other = new ToJsonPropertyToColumnMapping(TEST_OBJECT.getExasolColumnName(),
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour());
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentName() {
        final ToJsonPropertyToColumnMapping other = new ToJsonPropertyToColumnMapping("otherName",
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentColumn() {
        final ToJsonPropertyToColumnMapping other = new ToJsonPropertyToColumnMapping(TEST_OBJECT.getExasolColumnName(),
                DocumentPathExpression.empty(), TEST_OBJECT.getLookupFailBehaviour());
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }
}