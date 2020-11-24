package com.exasol.adapter.document.documentfetcher.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.document.querypredicate.AbstractComparisonPredicate.Operator;
import com.exasol.adapter.document.querypredicate.ColumnLiteralComparisonPredicate;

class ComparisonNegatorTest {

    @CsvSource({ //
            "EQUAL, NOT_EQUAL", //
            "NOT_EQUAL, EQUAL", //
            "LESS, GREATER_EQUAL", //
            "GREATER_EQUAL, LESS", //
            "GREATER, LESS_EQUAL", //
            "LIKE, NOT_LIKE",//
    })
    @ParameterizedTest
    void testNegation(final Operator input, final Operator expected) {
        final Operator negatedOperator = runNegation(input);
        assertThat(negatedOperator, equalTo(expected));
    }

    private Operator runNegation(final Operator input) {
        final ColumnLiteralComparisonPredicate comparison = new ColumnLiteralComparisonPredicate(input, null, null);
        final Operator negatedOperator = new ComparisonNegator().negate(comparison).getOperator();
        return negatedOperator;
    }

    @Test
    void testInLoop() {
        for (final Operator operator : Operator.values()) {
            assertThat(runNegation(runNegation(operator)), equalTo(operator));
        }
    }
}