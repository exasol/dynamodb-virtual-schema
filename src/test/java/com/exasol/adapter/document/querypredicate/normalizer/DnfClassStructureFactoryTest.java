package com.exasol.adapter.document.querypredicate.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Iterator;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.querypredicate.LogicalOperator;
import com.exasol.adapter.document.querypredicate.NoPredicate;
import com.exasol.adapter.document.querypredicate.NotPredicate;

class DnfClassStructureFactoryTest {
    private static final DnfClassStructureFactory FACTORY = new DnfClassStructureFactory();

    @Test
    void testBuildAnd() {
        final DnfOr or = FACTORY.build(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES);
        final DnfAnd and = or.getOperands().iterator().next();
        assertThat(and.asQueryPredicate(), equalTo(SelectionsConstants.AND_OF_TWO_DIFFERENT_PREDICATES));
    }

    @Test
    void testBuildOr() {
        final DnfOr or = FACTORY.build(SelectionsConstants.OR_OF_TWO_DIFFERENT_PREDICATES);
        final Iterator<DnfAnd> iterator = or.getOperands().iterator();
        final DnfAnd and1 = iterator.next();
        final DnfAnd and2 = iterator.next();
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(2)),
                () -> assertThat(and1.getOperands().size(), equalTo(1)),
                () -> assertThat(and2.getOperands().size(), equalTo(1))//
        );
    }

    @Test
    void testBuildNestedAnd() {
        final DnfOr or = FACTORY.build(SelectionsConstants.NESTED_AND);
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(2)), () -> {
                    final LogicalOperator queryPredicateOr = (LogicalOperator) or.asQueryPredicate();
                    assertThat(queryPredicateOr.getOperator(), equalTo(LogicalOperator.Operator.OR));
                }//
        );
    }

    @Test
    void testBuildComparison() {
        final DnfOr or = FACTORY.build(SelectionsConstants.EQUAL1);
        final DnfAnd and = or.getOperands().iterator().next();
        final DnfComparison comparison = and.getOperands().iterator().next();
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
        final NoPredicate noPredicate = new NoPredicate();
        final DnfOr or = FACTORY.build(noPredicate);
        assertAll(//
                () -> assertThat(or.getOperands().size(), equalTo(0)),
                () -> assertThat(or.asQueryPredicate(), equalTo(noPredicate))//
        );
    }

    @Test
    void testBuildNot() {
        final NotPredicate notPredicate = new NotPredicate(SelectionsConstants.EQUAL1);
        final DnfOr or = FACTORY.build(notPredicate);
        final DnfAnd and = or.getOperands().iterator().next();
        final DnfComparison comparison = and.getOperands().iterator().next();
        assertAll(//
                () -> assertThat(comparison.isNegated(), equalTo(true)),
                () -> assertThat(comparison.asQueryPredicate(), equalTo(notPredicate))//
        );
    }

    @Test
    void testBuildNotNot() {
        final NotPredicate notNotPredicate = new NotPredicate(new NotPredicate(SelectionsConstants.EQUAL1));
        final DnfOr or = FACTORY.build(notNotPredicate);
        final DnfAnd and = or.getOperands().iterator().next();
        final DnfComparison comparison = and.getOperands().iterator().next();
        assertThat(comparison.isNegated(), equalTo(false));
    }
}