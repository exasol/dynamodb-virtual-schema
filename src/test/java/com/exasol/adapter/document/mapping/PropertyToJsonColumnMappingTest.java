package com.exasol.adapter.document.mapping;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;

import org.junit.jupiter.api.Test;

class PropertyToJsonColumnMappingTest {

    private static final PropertyToJsonColumnMapping TEST_OBJECT = getDefaultTestObjectBuilder().build();

    private static PropertyToJsonColumnMapping.Builder getDefaultTestObjectBuilder() {
        return configureExampleMapping(PropertyToJsonColumnMapping.builder())//
                .varcharColumnSize(10)//
                .overflowBehaviour(MappingErrorBehaviour.ABORT);
    }

    @Test
    void testIdentical() {
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, TEST_OBJECT);
    }

    @Test
    void testEqual() {
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, getDefaultTestObjectBuilder().build());
    }

    @Test
    void testNotEqualWithDifferentName() {
        final ColumnMapping other = TEST_OBJECT.withNewExasolName("different_name");
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentVarcharSize() {
        final PropertyToJsonColumnMapping other = getDefaultTestObjectBuilder().varcharColumnSize(12).build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentOverflowBehaviour() {
        final PropertyToJsonColumnMapping other = getDefaultTestObjectBuilder()
                .overflowBehaviour(MappingErrorBehaviour.NULL).build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testNotEqualWithDifferentClass() {
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, new Object());
    }
}