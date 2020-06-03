package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.NotPredicate;

class DnfClassStructureFactoryTest {
    private static final DnfClassStructureFactory<Object> FACTORY = new DnfClassStructureFactory<>();

    @Test
    void testBuildAnd() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES);
        final DnfAnd<Object> and = or.getOperands().get(0);
        assertThat(and.asQueryPredicate(), equalTo(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES));
    }

    @Test
    void testBuildOr() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.OR_OF_TWO_DIFFERENT_PREDICATES);
        final DnfAnd<Object> and1 = or.getOperands().get(0);
        final DnfAnd<Object> and2 = or.getOperands().get(1);
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(2)),
                () -> assertThat(and1.getOperands().size(), equalTo(1)),
                () -> assertThat(and2.getOperands().size(), equalTo(1))//
        );
    }

    @Test
    void testBuildNestedAnd() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.NESTED_AND);
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(2)),
                () -> assertThat(or.asQueryPredicate().toString(), equalTo(
                        "((publisher=MockValueNode{value='test2'}) OR (isbn=MockValueNode{value='test'} AND publisher=MockValueNode{value='test'}))"))//
        );
    }

    @Test
    void testBuildComparison() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.EQUAL1);
        final DnfAnd<Object> and = or.getOperands().get(0);
        final DnfComparison<Object> comparison = and.getOperands().get(0);
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(1)),
                () -> assertThat(and.getOperands().size(), equalTo(1)),
                () -> assertThat(comparison.getComparisonPredicate(), equalTo(SelectionsConstants.EQUAL1)),
                () -> assertThat(comparison.isNegated(), equalTo(false)),
                () -> assertThat(comparison.asQueryPredicate(), equalTo(SelectionsConstants.EQUAL1))//
        );
    }

    @Test
    void testBuildNoPredicate() {
        final NoPredicate<Object> noPredicate = new NoPredicate<>();
        final DnfOr<Object> or = FACTORY.build(noPredicate);
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(0)),
                () -> assertThat(or.asQueryPredicate(), equalTo(noPredicate))//
        );
    }

    @Test
    void testBuildNot() {
        final NotPredicate<Object> notPredicate = new NotPredicate<>(SelectionsConstants.EQUAL1);
        final DnfOr<Object> or = FACTORY.build(notPredicate);
        final DnfAnd<Object> and = or.getOperands().get(0);
        final DnfComparison<Object> comparison = and.getOperands().get(0);
        assertAll(//
                () -> assertThat(comparison.isNegated(), equalTo(true)),
                () -> assertThat(comparison.asQueryPredicate(), equalTo(notPredicate))//
        );
    }

    @Test
    void testBuildNotNot() {
        final NotPredicate<Object> notNotPredicate = new NotPredicate<>(new NotPredicate<>(SelectionsConstants.EQUAL1));
        final DnfOr<Object> or = FACTORY.build(notNotPredicate);
        final DnfAnd<Object> and = or.getOperands().get(0);
        final DnfComparison<Object> comparison = and.getOperands().get(0);
        assertThat(comparison.isNegated(), equalTo(false));
    }
}