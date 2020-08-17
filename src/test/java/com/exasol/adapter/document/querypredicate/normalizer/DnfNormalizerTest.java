package com.exasol.adapter.document.querypredicate.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.querypredicate.LogicalOperator;

class DnfNormalizerTest {
    private static final DnfNormalizer NORMALIZER = new DnfNormalizer();

    @Test
    void testNormalization() {
        final DnfOr dnf = NORMALIZER.normalize(new LogicalOperator(
                Set.of(SelectionsConstants.EQUAL3, new LogicalOperator(
                        Set.of(SelectionsConstants.EQUAL1, SelectionsConstants.EQUAL2), LogicalOperator.Operator.OR)),
                LogicalOperator.Operator.AND));
        assertThat(dnf.asQueryPredicate(),
                equalTo(new LogicalOperator(Set.of(
                        new LogicalOperator(Set.of(SelectionsConstants.EQUAL3, SelectionsConstants.EQUAL1),
                                LogicalOperator.Operator.AND),
                        new LogicalOperator(Set.of(SelectionsConstants.EQUAL3, SelectionsConstants.EQUAL2),
                                LogicalOperator.Operator.AND)),
                        LogicalOperator.Operator.OR)));
    }
}