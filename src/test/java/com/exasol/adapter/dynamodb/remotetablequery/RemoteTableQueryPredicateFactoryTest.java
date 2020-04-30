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
import com.exasol.adapter.dynamodb.literalconverter.NotLiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;

class QueryPredicateFactoryTest {
    private static final AbstractColumnMappingDefinition COLUMN_MAPPING = new ToJsonColumnMappingDefinition(
            new AbstractColumnMappingDefinition.ConstructorParameters("name", null, null));
    private static final DocumentValue<Object> LITERAL = (DocumentValue<Object>) visitor -> {
    };
    private static final SqlLiteralToDocumentValueConverter<Object> LITERAL_FACTORY = new SqlLiteralToDocumentValueConverter<Object>() {
        @Override
        public DocumentValue<Object> convert(final SqlNode exasolLiteralNode) throws NotLiteralException {
            return LITERAL;
        }
    };
    private static final QueryPredicateFactory<Object> FACTORY = new QueryPredicateFactory<Object>(LITERAL_FACTORY);
    private static ColumnMetadata columnMetadata;
    private static SqlNode validColumnLiteralEqualityPredicate;

    @BeforeAll
    static void setup() throws IOException {
        columnMetadata = new SchemaMappingDefinitionToSchemaMetadataConverter().convertColumn(COLUMN_MAPPING);
        validColumnLiteralEqualityPredicate = new SqlPredicateEqual(new SqlLiteralString("test"),
                new SqlColumn(0, columnMetadata));
    }

    @Test
    void testBuildNoPredicate() {
        assertThat(FACTORY.buildPredicateFor(null), instanceOf(NoPredicate.class));
    }

    @Test
    void testBuildAndPredicate() {
        final BinaryLogicalOperator<Object> binaryLogicalOperator = (BinaryLogicalOperator<Object>) FACTORY
                .buildPredicateFor(new SqlPredicateAnd(List.of(validColumnLiteralEqualityPredicate)));
        assertAll(//
                () -> assertThat(binaryLogicalOperator.getOperands().size(), equalTo(1)),
                () -> assertThat(binaryLogicalOperator.getOperands().get(0),
                        instanceOf(ColumnLiteralComparisonPredicate.class)),
                () -> assertThat(binaryLogicalOperator.getOperator(), equalTo(BinaryLogicalOperator.Operator.AND))//
        );
    }

    @Test
    void testBuildOrPredicate() {
        final BinaryLogicalOperator<Object> binaryLogicalOperator = (BinaryLogicalOperator<Object>) FACTORY
                .buildPredicateFor(new SqlPredicateOr(List.of(validColumnLiteralEqualityPredicate)));
        assertAll(//
                () -> assertThat(binaryLogicalOperator.getOperands().size(), equalTo(1)),
                () -> assertThat(binaryLogicalOperator.getOperands().get(0),
                        instanceOf(ColumnLiteralComparisonPredicate.class)),
                () -> assertThat(binaryLogicalOperator.getOperator(), equalTo(BinaryLogicalOperator.Operator.OR))//
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
}