package com.exasol.adapter.dynamodb.querypredicate.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Iterator;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.querypredicate.LogicalOperator;
import com.exasol.adapter.dynamodb.querypredicate.NoPredicate;
import com.exasol.adapter.dynamodb.querypredicate.NotPredicate;

class DnfClassStructureFactoryTest {
    private static final DnfClassStructureFactory<Object> FACTORY = new DnfClassStructureFactory<>();

    @Test
    void testBuildAnd() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES);
        final DnfAnd<Object> and = or.getOperands().iterator().next();
        assertThat(and.asQueryPredicate(), equalTo(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES));
    }

    @Test
    void testBuildOr() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.OR_OF_TWO_DIFFERENT_PREDICATES);
        final Iterator<DnfAnd<Object>> iterator = or.getOperands().iterator();
        final DnfAnd<Object> and1 = iterator.next();
        final DnfAnd<Object> and2 = iterator.next();
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
                () -> assertThat(or.getOperands().size(), equalTo(2)), () -> {
                    final LogicalOperator<Object> queryPredicateOr = (LogicalOperator<Object>) or.asQueryPredicate();
                    assertThat(queryPredicateOr.getOperator(), equalTo(LogicalOperator.Operator.OR));
                }//
        );
    }

    @Test
    void testBuildComparison() {
        final DnfOr<Object> or = FACTORY.build(SelectionsConstants.EQUAL1);
        final DnfAnd<Object> and = or.getOperands().iterator().next();
        final DnfComparison<Object> comparison = and.getOperands().iterator().next();
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
        final DnfAnd<Object> and = or.getOperands().iterator().next();
        final DnfComparison<Object> comparison = and.getOperands().iterator().next();
        assertAll(//
                () -> assertThat(comparison.isNegated(), equalTo(true)),
                () -> assertThat(comparison.asQueryPredicate(), equalTo(notPredicate))//
        );
    }

    @Test
    void testBuildNotNot() {
        final NotPredicate<Object> notNotPredicate = new NotPredicate<>(new NotPredicate<>(SelectionsConstants.EQUAL1));
        final DnfOr<Object> or = FACTORY.build(notNotPredicate);
        final DnfAnd<Object> and = or.getOperands().iterator().next();
        final DnfComparison<Object> comparison = and.getOperands().iterator().next();
        assertThat(comparison.isNegated(), equalTo(false));
    }
}