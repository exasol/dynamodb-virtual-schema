package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.MockValueNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.documentpath.PathIterationStateProvider;
import com.exasol.sql.expression.IntegerLiteral;

class IterationIndexColumnValueExtractorTest {
    private static final DocumentPathExpression TABLES_PATH = new DocumentPathExpression.Builder()
            .addObjectLookup("test").addArrayAll().build();
    private static final IterationIndexColumnMapping COLUMN = new IterationIndexColumnMapping("INDEX", TABLES_PATH);
    private static final IterationIndexColumnValueExtractor<Object> EXTRACTOR = new IterationIndexColumnValueExtractor<>(
            COLUMN);
    private static final int ITERATION_INDEX = 14;
    PathIterationStateProvider ITERATION_STATE_PROVIDER = new PathIterationStateProvider() {
        @Override
        public int getIndexFor(final DocumentPathExpression pathToArrayAll) {
            if (pathToArrayAll.equals(TABLES_PATH)) {
                return ITERATION_INDEX;
            }
            return -1;
        }
    };

    @Test
    void testExtractColumnValue() {
        final IntegerLiteral intValue = (IntegerLiteral) EXTRACTOR.extractColumnValue(new MockValueNode(""),
                this.ITERATION_STATE_PROVIDER);
        assertThat(intValue.getValue(), equalTo(ITERATION_INDEX));
    }
}