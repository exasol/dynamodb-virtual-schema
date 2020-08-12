package com.exasol.adapter.dynamodb.mapping;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

class PropertyToJsonColumnMappingTest {

    private static final PropertyToJsonColumnMapping TEST_OBJECT = new PropertyToJsonColumnMapping("name",
            DocumentPathExpression.builder().addArrayAll().build(), MappingErrorBehaviour.ABORT, 10,
            MappingErrorBehaviour.ABORT);

    @Test
    void testEqual() {
        final PropertyToJsonColumnMapping other = new PropertyToJsonColumnMapping(TEST_OBJECT.getExasolColumnName(),
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getMappingErrorBehaviour(), 10,
                MappingErrorBehaviour.ABORT);
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentName() {
        final PropertyToJsonColumnMapping other = new PropertyToJsonColumnMapping("otherName",
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getMappingErrorBehaviour(), 10,
                MappingErrorBehaviour.ABORT);
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentColumn() {
        final PropertyToJsonColumnMapping other = new PropertyToJsonColumnMapping(TEST_OBJECT.getExasolColumnName(),
                DocumentPathExpression.empty(), TEST_OBJECT.getMappingErrorBehaviour(), 10,
                MappingErrorBehaviour.ABORT);
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }
}