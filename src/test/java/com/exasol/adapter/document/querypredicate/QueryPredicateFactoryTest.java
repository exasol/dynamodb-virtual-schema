package com.exasol.adapter.document.querypredicate;

import static com.exasol.adapter.document.mapping.PropertyToColumnMappingBuilderQuickAccess.configureExampleMapping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.MappingErrorBehaviour;
import com.exasol.adapter.document.mapping.PropertyToJsonColumnMapping;
import com.exasol.adapter.document.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;

class QueryPredicateFactoryTest {
    private static final ColumnMapping COLUMN_MAPPING = configureExampleMapping(PropertyToJsonColumnMapping.builder())
            .varcharColumnSize(0).overflowBehaviour(MappingErrorBehaviour.NULL).build();
    private static final SqlLiteralString LITERAL = new SqlLiteralString("test");
    private static final QueryPredicateFactory FACTORY = QueryPredicateFactory.getInstance();
    private static ColumnMetadata columnMetadata;
    private static SqlNode validColumnLiteralEqualityPredicate;

    @BeforeAll
    static void setup() throws IOException {
        columnMetadata = new SchemaMappingToSchemaMetadataConverter().convertColumn(COLUMN_MAPPING);
        validColumnLiteralEqualityPredicate = new SqlPredicateEqual(LITERAL, new SqlColumn(0, columnMetadata));
    }

    @Test
    void testBuildNoPredicate() {
        assertThat(FACTORY.buildPredicateFor(null), instanceOf(NoPredicate.class));
    }

    @Test
    void testBuildAndPredicate() {
        final LogicalOperator logicalOperator = (LogicalOperator) FACTORY
                .buildPredicateFor(new SqlPredicateAnd(List.of(validColumnLiteralEqualityPredicate)));
        assertAll(//
                () -> assertThat(logicalOperator.getOperands().size(), equalTo(1)),
                () -> assertThat(logicalOperator.getOperands().iterator().next(),
                        instanceOf(ColumnLiteralComparisonPredicate.class)),
                () -> assertThat(logicalOperator.getOperator(), equalTo(LogicalOperator.Operator.AND))//
        );
    }

    @Test
    void testBuildOrPredicate() {
        final LogicalOperator logicalOperator = (LogicalOperator) FACTORY
                .buildPredicateFor(new SqlPredicateOr(List.of(validColumnLiteralEqualityPredicate)));
        assertAll(//
                () -> assertThat(logicalOperator.getOperands().size(), equalTo(1)),
                () -> assertThat(logicalOperator.getOperands().iterator().next(),
                        instanceOf(ColumnLiteralComparisonPredicate.class)),
                () -> assertThat(logicalOperator.getOperator(), equalTo(LogicalOperator.Operator.OR))//
        );
    }

    @Test
    void testBuildColumnLiteralEqualityPredicate() {
        final ColumnLiteralComparisonPredicate predicate = (ColumnLiteralComparisonPredicate) FACTORY
                .buildPredicateFor(validColumnLiteralEqualityPredicate);
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(AbstractComparisonPredicate.Operator.EQUAL))//
        );
    }

    @Test
    void testBuildColumnLiteralEqualityPredicateWithSwappedParameters() {
        final ColumnLiteralComparisonPredicate predicate = (ColumnLiteralComparisonPredicate) FACTORY
                .buildPredicateFor(new SqlPredicateEqual(new SqlColumn(0, columnMetadata), LITERAL));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(AbstractComparisonPredicate.Operator.EQUAL))//
        );
    }

    @Test
    void testBuildColumnLiteralEqualityPredicateFailsForTwoColumns() {
        final SqlPredicateEqual predicate = new SqlPredicateEqual(new SqlColumn(0, columnMetadata),
                new SqlColumn(1, columnMetadata));
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> FACTORY.buildPredicateFor(predicate));
        assertThat(exception.getMessage(),
                equalTo("Predicates on two columns are not yet supported in this Virtual Schema version."));
    }

    @Test
    void testBuildColumnLiteralEqualityPredicateFailsForTwoLiterals() {
        final SqlPredicateEqual predicate = new SqlPredicateEqual(LITERAL, LITERAL);
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> FACTORY.buildPredicateFor(predicate));
        assertThat(exception.getMessage(),
                equalTo("Predicates on two literals are not yet supported in this Virtual Schema version."));
    }

    @Test
    void testBuildColumnLiteralLessPredicate() {
        final ColumnLiteralComparisonPredicate predicate = (ColumnLiteralComparisonPredicate) FACTORY
                .buildPredicateFor(new SqlPredicateLess(new SqlColumn(0, columnMetadata), LITERAL));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(AbstractComparisonPredicate.Operator.LESS))//
        );
    }

    @Test
    void testBuildColumnLiteralLessPredicateWithSwappedParameters() {
        final ColumnLiteralComparisonPredicate predicate = (ColumnLiteralComparisonPredicate) FACTORY
                .buildPredicateFor(new SqlPredicateLess(LITERAL, new SqlColumn(0, columnMetadata)));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(AbstractComparisonPredicate.Operator.GREATER))//
        );
    }

    @Test
    void testBuildColumnLiteralLessEqualPredicate() {
        final ColumnLiteralComparisonPredicate predicate = (ColumnLiteralComparisonPredicate) FACTORY
                .buildPredicateFor(new SqlPredicateLessEqual(new SqlColumn(0, columnMetadata), LITERAL));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(AbstractComparisonPredicate.Operator.LESS_EQUAL))//
        );
    }

    @Test
    void testBuildColumnLiteralLessEqualPredicateWithSwappedParameters() {
        final ColumnLiteralComparisonPredicate predicate = (ColumnLiteralComparisonPredicate) FACTORY
                .buildPredicateFor(new SqlPredicateLessEqual(LITERAL, new SqlColumn(0, columnMetadata)));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(AbstractComparisonPredicate.Operator.GREATER_EQUAL))//
        );
    }

    @Test
    void testBuildNot() {
        final SqlPredicateNot sqlNot = new SqlPredicateNot(validColumnLiteralEqualityPredicate);
        final QueryPredicate queryPredicate = FACTORY.buildPredicateFor(sqlNot);
        assertThat(queryPredicate, instanceOf(NotPredicate.class));

    }
}