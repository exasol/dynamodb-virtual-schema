package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;

class ToJsonPropertyToColumnMappingTest {

    private static final ToJsonPropertyToColumnMapping TEST_OBJECT = new ToJsonPropertyToColumnMapping("name",
            new DocumentPathExpression.Builder().addArrayAll().build(), LookupFailBehaviour.EXCEPTION);

    @Test
    void testEqual() {
        final ToJsonPropertyToColumnMapping other = new ToJsonPropertyToColumnMapping(TEST_OBJECT.getExasolColumnName(),
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour());
        assertAll(//
                () -> assertThat(TEST_OBJECT.equals(other), equalTo(true)),
                () -> assertThat(TEST_OBJECT.hashCode() == other.hashCode(), equalTo(true))//
        );
    }

    @Test
    void testNotEqualWithDifferentName() {
        final ToJsonPropertyToColumnMapping other = new ToJsonPropertyToColumnMapping("otherName",
                TEST_OBJECT.getPathToSourceProperty(), TEST_OBJECT.getLookupFailBehaviour());
        assertAll(//
                () -> assertThat(TEST_OBJECT.equals(other), equalTo(false)),
                () -> assertThat(TEST_OBJECT.hashCode() == other.hashCode(), equalTo(false))//
        );
    }

    @Test
    void testNotEqualWithDifferentColumn() {
        final ToJsonPropertyToColumnMapping other = new ToJsonPropertyToColumnMapping(TEST_OBJECT.getExasolColumnName(),
                DocumentPathExpression.empty(), TEST_OBJECT.getLookupFailBehaviour());
        assertAll(//
                () -> assertThat(TEST_OBJECT.equals(other), equalTo(false)),
                () -> assertThat(TEST_OBJECT.hashCode() == other.hashCode(), equalTo(false))//
        );
    }
}