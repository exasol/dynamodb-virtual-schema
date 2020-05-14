package com.exasol.adapter.dynamodb.remotetablequery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.ToJsonPropertyToColumnMapping;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;

class QueryPredicateFactoryTest {
    private static final ColumnMapping COLUMN_MAPPING = new ToJsonPropertyToColumnMapping("name", null, null);
    private static final DocumentValue<Object> LITERAL = (DocumentValue<Object>) visitor -> {
    };
    private static final SqlLiteralToDocumentValueConverter<Object> LITERAL_FACTORY = exasolLiteralNode -> LITERAL;
    private static final QueryPredicateFactory<Object> FACTORY = new QueryPredicateFactory<>(LITERAL_FACTORY);
    private static ColumnMetadata columnMetadata;
    private static SqlNode validColumnLiteralEqualityPredicate;

    @BeforeAll
    static void setup() throws IOException {
        columnMetadata = new SchemaMappingToSchemaMetadataConverter().convertColumn(COLUMN_MAPPING);
        validColumnLiteralEqualityPredicate = new SqlPredicateEqual(new SqlLiteralString("test"),
                new SqlColumn(0, columnMetadata));
    }

    @Test
    void testBuildNoPredicate() {
        assertThat(FACTORY.buildPredicateFor(null), instanceOf(NoPredicate.class));
    }

    @Test
    void testBuildAndPredicate() {
        final LogicalOperator<Object> logicalOperator = (LogicalOperator<Object>) FACTORY
                .buildPredicateFor(new SqlPredicateAnd(List.of(validColumnLiteralEqualityPredicate)));
        assertAll(//
                () -> assertThat(logicalOperator.getOperands().size(), equalTo(1)),
                () -> assertThat(logicalOperator.getOperands().get(0),
                        instanceOf(ColumnLiteralComparisonPredicate.class)),
                () -> assertThat(logicalOperator.getOperator(), equalTo(LogicalOperator.Operator.AND))//
        );
    }

    @Test
    void testBuildOrPredicate() {
        final LogicalOperator<Object> logicalOperator = (LogicalOperator<Object>) FACTORY
                .buildPredicateFor(new SqlPredicateOr(List.of(validColumnLiteralEqualityPredicate)));
        assertAll(//
                () -> assertThat(logicalOperator.getOperands().size(), equalTo(1)),
                () -> assertThat(logicalOperator.getOperands().get(0),
                        instanceOf(ColumnLiteralComparisonPredicate.class)),
                () -> assertThat(logicalOperator.getOperator(), equalTo(LogicalOperator.Operator.OR))//
        );
    }

    @Test
    void testBuildColumnLiteralEqualityPredicate() {
        final ColumnLiteralComparisonPredicate<Object> predicate = (ColumnLiteralComparisonPredicate<Object>) FACTORY
                .buildPredicateFor(validColumnLiteralEqualityPredicate);
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(ComparisonPredicate.Operator.EQUAL))//
        );
    }

    @Test
    void testBuildColumnLiteralEqualityPredicateWithSwappedParameters() {
        final ColumnLiteralComparisonPredicate<Object> predicate = (ColumnLiteralComparisonPredicate<Object>) FACTORY
                .buildPredicateFor(
                        new SqlPredicateEqual(new SqlColumn(0, columnMetadata), new SqlLiteralString("test")));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(ComparisonPredicate.Operator.EQUAL))//
        );
    }

    @Test
    void testBuildColumnLiteralEqualityPredicateFailsForTwoColumns() {
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> FACTORY.buildPredicateFor(
                        new SqlPredicateEqual(new SqlColumn(0, columnMetadata), new SqlColumn(1, columnMetadata))));
        assertThat(exception.getMessage(),
                equalTo("Predicates on two columns are not yet supported in this Virtual Schema version."));
    }

    @Test
    void testBuildColumnLiteralEqualityPredicateFailsForTwoLiterals() {
        final UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> FACTORY
                .buildPredicateFor(new SqlPredicateEqual(new SqlLiteralString("test"), new SqlLiteralString("test"))));
        assertThat(exception.getMessage(),
                equalTo("Predicates on two literals are not yet supported in this Virtual Schema version."));
    }

    @Test
    void testBuildColumnLiteralLessPredicate() {
        final ColumnLiteralComparisonPredicate<Object> predicate = (ColumnLiteralComparisonPredicate<Object>) FACTORY
                .buildPredicateFor(
                        new SqlPredicateLess(new SqlColumn(0, columnMetadata), new SqlLiteralString("test")));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(ComparisonPredicate.Operator.LESS))//
        );
    }

    @Test
    void testBuildColumnLiteralLessPredicateWithSwappedParameters() {
        final ColumnLiteralComparisonPredicate<Object> predicate = (ColumnLiteralComparisonPredicate<Object>) FACTORY
                .buildPredicateFor(
                        new SqlPredicateLess(new SqlLiteralString("test"), new SqlColumn(0, columnMetadata)));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(ComparisonPredicate.Operator.GREATER))//
        );
    }

    @Test
    void testBuildColumnLiteralLessEqualPredicate() {
        final ColumnLiteralComparisonPredicate<Object> predicate = (ColumnLiteralComparisonPredicate<Object>) FACTORY
                .buildPredicateFor(
                        new SqlPredicateLessEqual(new SqlColumn(0, columnMetadata), new SqlLiteralString("test")));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(ComparisonPredicate.Operator.LESS_EQUAL))//
        );
    }

    @Test
    void testBuildColumnLiteralLessEqualPredicateWithSwappedParameters() {
        final ColumnLiteralComparisonPredicate<Object> predicate = (ColumnLiteralComparisonPredicate<Object>) FACTORY
                .buildPredicateFor(
                        new SqlPredicateLessEqual(new SqlLiteralString("test"), new SqlColumn(0, columnMetadata)));
        assertAll(//
                () -> assertThat(predicate.getLiteral(), equalTo(LITERAL)),
                () -> assertThat(predicate.getColumn().getExasolColumnName(),
                        equalTo(COLUMN_MAPPING.getExasolColumnName())),
                () -> assertThat(predicate.getOperator(), equalTo(ComparisonPredicate.Operator.GREATER_EQUAL))//
        );
    }
}