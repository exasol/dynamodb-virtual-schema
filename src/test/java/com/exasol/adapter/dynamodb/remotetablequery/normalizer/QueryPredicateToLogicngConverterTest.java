package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;

class QueryPredicateToLogicngConverterTest {
    private static final QueryPredicateToLogicngConverter<Object> CONVERTER = new QueryPredicateToLogicngConverter<>();

    @Test
    void testConvertNoPredicate() {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER.convert(new NoPredicate<>());
        assertAll(//
                () -> assertThat(result.getLogicngFormula().toString(), equalTo("$true")),
                () -> assertThat(result.getVariablesMapping().isEmpty(), equalTo(true))//
        );
    }

    @Test
    void testConvertComparison() {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER.convert(SelectionsConstants.EQUAL1);
        assertAll(//
                () -> assertThat(result.getLogicngFormula().toString(), equalTo("Variable0")),
                () -> assertThat(result.getVariablesMapping().values().stream().findFirst().get(),
                        equalTo(SelectionsConstants.EQUAL1))//
        );
    }

    @Test
    void testConvertAnd() {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER
                .convert(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES);
        assertThat(result.getLogicngFormula().toString(), equalTo("Variable0 & Variable1"));
    }

    @Test
    void testConvertAndOfIdenticalPredicates() {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER
                .convert(SelectionsConstants.AND_OF_TWO_IDENTICAL_PREDICATES);
        assertThat(result.getLogicngFormula().toString(), equalTo("Variable0"));
    }

    @Test
    void testConvertOr() {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER
                .convert(SelectionsConstants.OR_OF_TWO_DIFFERENT_PREDICATES);
        assertThat(result.getLogicngFormula().toString(), equalTo("Variable0 | Variable1"));
    }

    @Test
    void testConvertNestedAnd() {
        final QueryPredicateToLogicngConverter.Result<Object> result = CONVERTER
                .convert(SelectionsConstants.NESTED_AND);
        assertThat(result.getLogicngFormula().toString(), equalTo("Variable0 | Variable1 & Variable2"));
    }
}