package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.querypredicate.NoPredicate;
import com.exasol.adapter.dynamodb.querypredicate.QueryPredicate;

class LogicngToQueryPredicateConverterTest {
    private static final QueryPredicateToLogicngConverter<Object> CONVERTER = new QueryPredicateToLogicngConverter<>();

    @Test
    void testConvertNoPredicate() {
        testLoopConversion(new NoPredicate<>());
    }

    @Test
    void testConvertComparison() {
        testLoopConversion(SelectionsConstants.EQUAL1);
    }

    @Test
    void testConvertAnd() {
        testLoopConversion(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES);
    }

    @Test
    void testConvertNestedAnd() {
        testLoopConversion(SelectionsConstants.NESTED_AND);
    }

    @Test
    void testConvertOr() {
        testLoopConversion(SelectionsConstants.OR_OF_TWO_DIFFERENT_PREDICATES);
    }

    @Test
    void testConvertNot() {
        testLoopConversion(SelectionsConstants.NOT_EQUAL1);
    }

    void testLoopConversion(final QueryPredicate<Object> predicate) {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER.convert(predicate);
        final LogicngToQueryPredicateConverter<Object> backConverter = new LogicngToQueryPredicateConverter<>(
                result.getVariablesMapping());
        assertThat(backConverter.convert(result.getLogicngFormula()), equalTo(predicate));
    }
}