package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.QueryPredicate;

class LogicngToQueryPredicateConverterTest {
    private static final QueryPredicateToLogicngConverter<Object> CONVERTER = new QueryPredicateToLogicngConverter<>();

    @Test
    void testConvertNoPredicate() {
        testLoopConversion(new NoPredicate<>());
    }

    @Test
    void testConvertComparison() {
        testLoopConversion(Selections.EQUAL1);
    }

    @Test
    void testConvertAnd() {
        testLoopConversion(Selections.AND_OF_TWO_DIFFERENT_PREDICATES);
    }

    @Test
    void testConvertNestedAnd() {
        testLoopConversion(Selections.NESTED_AND);
    }

    @Test
    void testConvertOr() {
        testLoopConversion(Selections.OR_OF_TWO_DIFFERENT_PREDICATES);
    }

    void testLoopConversion(final QueryPredicate<Object> predicate) {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER.convert(predicate);
        final LogicngToQueryPredicateConverter<Object> backConverter = new LogicngToQueryPredicateConverter<>(
                result.getVariablesMapping());
        assertThat(backConverter.convert(result.getLogicngFormula()), equalTo(predicate));
    }
}