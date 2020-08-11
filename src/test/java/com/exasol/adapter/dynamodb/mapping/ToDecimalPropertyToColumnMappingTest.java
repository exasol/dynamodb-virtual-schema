package com.exasol.adapter.dynamodb.mapping;

import static com.exasol.EqualityMatchers.assertSymmetricEqualWithHashAndEquals;
import static com.exasol.EqualityMatchers.assertSymmetricNotEqualWithHashAndEquals;
import static com.exasol.adapter.dynamodb.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.metadata.DataType;

class ToDecimalPropertyToColumnMappingTest {
    private static final ToDecimalPropertyToColumnMapping TEST_OBJECT = configureExampleMapping(
            ToDecimalPropertyToColumnMapping.builder())//
                    .decimalPrecision(12)//
                    .decimalScale(1)//
                    .overflowBehaviour(MappingErrorBehaviour.NULL)//
                    .notANumberBehaviour(MappingErrorBehaviour.NULL)//
                    .build();

    @Test
    void testGetters() {
        assertAll(//
                () -> assertThat(TEST_OBJECT.getDecimalPrecision(), equalTo(12)),
                () -> assertThat(TEST_OBJECT.getDecimalScale(), equalTo(1)),
                () -> assertThat(TEST_OBJECT.getOverflowBehaviour(), equalTo(MappingErrorBehaviour.NULL)),
                () -> assertThat(TEST_OBJECT.getExasolDataType(), equalTo(DataType.createDecimal(12, 1))),
                () -> assertThat(TEST_OBJECT.getNotANumberBehaviour(), equalTo(MappingErrorBehaviour.NULL))//
        );
    }

    @Test
    void testEquality() {
        final ColumnMapping other = TEST_OBJECT.withNewExasolName(TEST_OBJECT.getExasolColumnName());
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testEqualityForIdentical() {
        assertSymmetricEqualWithHashAndEquals(TEST_OBJECT, TEST_OBJECT);
    }

    @Test
    void testInequalityByPrecision() {
        final ToDecimalPropertyToColumnMapping other = configureExampleMapping(
                ToDecimalPropertyToColumnMapping.builder())//
                        .decimalPrecision(11)// single difference
                        .decimalScale(1)//
                        .overflowBehaviour(MappingErrorBehaviour.NULL)//
                        .notANumberBehaviour(MappingErrorBehaviour.NULL)//
                        .build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testInequalityByScale() {
        final ToDecimalPropertyToColumnMapping other = configureExampleMapping(
                ToDecimalPropertyToColumnMapping.builder())//
                        .decimalPrecision(12)//
                        .decimalScale(2)// single difference
                        .overflowBehaviour(MappingErrorBehaviour.NULL)//
                        .notANumberBehaviour(MappingErrorBehaviour.NULL)//
                        .build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testInequalityByOverflowBehaviour() {
        final ToDecimalPropertyToColumnMapping other = configureExampleMapping(
                ToDecimalPropertyToColumnMapping.builder())//
                        .decimalPrecision(12)//
                        .decimalScale(1)//
                        .overflowBehaviour(MappingErrorBehaviour.ABORT)// single difference
                        .notANumberBehaviour(MappingErrorBehaviour.NULL)//
                        .build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testInequalityByNotANumberBehaviour() {
        final ToDecimalPropertyToColumnMapping other = configureExampleMapping(
                ToDecimalPropertyToColumnMapping.builder())//
                        .decimalPrecision(12)//
                        .decimalScale(1)//
                        .overflowBehaviour(MappingErrorBehaviour.NULL)//
                        .notANumberBehaviour(MappingErrorBehaviour.ABORT)// single difference
                        .build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testInequalityBySuper() {
        final ToDecimalPropertyToColumnMapping other = configureExampleMapping(
                ToDecimalPropertyToColumnMapping.builder())//
                        .exasolColumnName("different name")// single difference
                        .decimalPrecision(12)//
                        .decimalScale(1)//
                        .overflowBehaviour(MappingErrorBehaviour.NULL)//
                        .notANumberBehaviour(MappingErrorBehaviour.NULL)//
                        .build();
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, other);
    }

    @Test
    void testInequalityByDifferentClass() {
        assertSymmetricNotEqualWithHashAndEquals(TEST_OBJECT, new Object());
    }
}