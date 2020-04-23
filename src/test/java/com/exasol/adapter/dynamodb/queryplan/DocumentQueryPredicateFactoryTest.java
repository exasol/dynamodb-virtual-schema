package com.exasol.adapter.dynamodb.queryplan;

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
import com.exasol.adapter.dynamodb.literalconverter.NotALiteralException;
import com.exasol.adapter.dynamodb.literalconverter.SqlLiteralToDocumentValueConverter;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.SchemaMappingDefinitionToSchemaMetadataConverter;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;

class DocumentQueryPredicateFactoryTest {
    private static final AbstractColumnMappingDefinition COLUMN_MAPPING = new ToJsonColumnMappingDefinition(
            new AbstractColumnMappingDefinition.ConstructorParameters("name", null, null));
    private static final DocumentValue<Object> LITERAL = (DocumentValue<Object>) visitor -> {
    };
    private static final SqlLiteralToDocumentValueConverter<Object> LITERAL_FACTORY = new SqlLiteralToDocumentValueConverter<Object>() {
        @Override
        public DocumentValue<Object> convert(final SqlNode exasolLiteralNode) throws NotALiteralException {
            return LITERAL;
        }
    };
    private static final DocumentQueryPredicateFactory<Object> FACTORY = new DocumentQueryPredicateFactory<Object>(
            LITERAL_FACTORY);
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
        final AndPredicate<Object> andPredicate = (AndPredicate<Object>) FACTORY
                .buildPredicateFor(new SqlPredicateAnd(List.of(validColumnLiteralEqualityPredicate)));
        assertThat(andPredicate.getAndedPredicates().size(), equalTo(1));
        assertThat(andPredicate.getAndedPredicates().get(0), instanceOf(ColumnLiteralComparisonPredicate.class));
    }

    @Test
    void testBuildOrPredicate() {
        final OrPredicate<Object> orPredicate = (OrPredicate<Object>) FACTORY
                .buildPredicateFor(new SqlPredicateOr(List.of(validColumnLiteralEqualityPredicate)));
        assertThat(orPredicate.getOredPredicates().size(), equalTo(1));
        assertThat(orPredicate.getOredPredicates().get(0), instanceOf(ColumnLiteralComparisonPredicate.class));
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