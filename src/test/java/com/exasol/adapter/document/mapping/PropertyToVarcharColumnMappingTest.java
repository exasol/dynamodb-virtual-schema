package com.exasol.adapter.document.mapping;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

class PropertyToVarcharColumnMappingTest {
    private static final int STRING_LENGTH = 10;

    private static PropertyToVarcharColumnMapping.Builder getDefaultTestObjectBuilder() {
        return configureExampleMapping(PropertyToVarcharColumnMapping.builder())//
                .varcharColumnSize(STRING_LENGTH)//
                .overflowBehaviour(TruncateableMappingErrorBehaviour.TRUNCATE);
    }

    @Test
    void testGetters() {
        final PropertyToVarcharColumnMapping testObject = getDefaultTestObjectBuilder().build();
        assertAll(
                () -> assertThat(testObject.getExasolDataType().toString(), equalTo("VARCHAR(10) UTF8")),
                () -> assertThat(testObject.isExasolColumnNullable(), equalTo(true)),
                () -> assertThat(testObject.getOverflowBehaviour(), equalTo(TruncateableMappingErrorBehaviour.TRUNCATE))//
        );
    }

    @Test
    void testIdentical() {
        final PropertyToVarcharColumnMapping testObject = getDefaultTestObjectBuilder().build();
        assertSymmetricEqualWithHashAndEquals(testObject, testObject);
    }

    @Test
    void testEqual() {
        assertSymmetricEqualWithHashAndEquals(getDefaultTestObjectBuilder().build(),
                getDefaultTestObjectBuilder().build());
    }

    @Test
    void testNotEqualBySuper() {
        final PropertyToVarcharColumnMapping testObject = getDefaultTestObjectBuilder().build();
        final ColumnMapping other = testObject.withNewExasolName("otherName");
        assertSymmetricNotEqualWithHashAndEquals(testObject, other);
    }

    @Test
    void testNotEqualByDifferentOverflowBehaviour() {
        final PropertyToVarcharColumnMapping testObject = getDefaultTestObjectBuilder().build();
        final PropertyToVarcharColumnMapping other = getDefaultTestObjectBuilder()
                .overflowBehaviour(TruncateableMappingErrorBehaviour.ABORT).build();
        assertSymmetricNotEqualWithHashAndEquals(testObject, other);
    }

    @Test
    void testNotEqualByDifferentStringSize() {
        final PropertyToVarcharColumnMapping testObject = getDefaultTestObjectBuilder().build();
        final PropertyToVarcharColumnMapping other = getDefaultTestObjectBuilder().varcharColumnSize(12).build();
        assertSymmetricNotEqualWithHashAndEquals(testObject, other);
    }

    @Test
    void testNotEqualByDifferentClass() {
        final PropertyToVarcharColumnMapping testObject = getDefaultTestObjectBuilder().build();
        assertSymmetricNotEqualWithHashAndEquals(testObject, new Object());
    }
}
