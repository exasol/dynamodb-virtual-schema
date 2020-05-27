package com.exasol.adapter.dynamodb.remotetablequery.normalizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.remotetablequery.LogicalOperator;

class DnfNormalizerTest {
    private static final DnfNormalizer<Object> NORMALIZER = new DnfNormalizer<>();

    @Test
    void testNormalization() {
        final DnfOr<Object> dnf = NORMALIZER.normalize(new LogicalOperator<>(
                List.of(SelectionsConstants.EQUAL3, new LogicalOperator<>(
                        List.of(SelectionsConstants.EQUAL1, SelectionsConstants.EQUAL2), LogicalOperator.Operator.OR)),
                LogicalOperator.Operator.AND));
        assertThat(dnf.toString(), equalTo(
                "((isbn=MockValueNode{value='test'} AND publisher=MockValueNode{value='test2'}) OR (publisher=MockValueNode{value='test'} AND publisher=MockValueNode{value='test2'}))"));
    }
}