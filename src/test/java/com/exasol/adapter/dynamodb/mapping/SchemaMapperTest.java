package com.exasol.adapter.dynamodb.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.DocumentNode;
import com.exasol.adapter.dynamodb.documentnode.MockArrayNode;
import com.exasol.adapter.dynamodb.documentnode.MockObjectNode;
import com.exasol.adapter.dynamodb.documentnode.MockValueNode;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

public class SchemaMapperTest {
    private static final StringLiteral STRING_LITERAL = StringLiteral.of("test");

    @Test
    public void testMapRow() {
        final ToJsonColumnMappingDefinition columnMapping = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("test", DocumentPathExpression.empty(),
                        LookupFailBehaviour.EXCEPTION));

        final TableMappingDefinition tableMapping = TableMappingDefinition.rootTableBuilder("table", "table")
                .withColumnMappingDefinition(columnMapping).build();
        final RemoteTableQuery<Object> remoteTableQuery = new RemoteTableQuery<>(tableMapping, List.of(columnMapping),
                new NoPredicate<>());
        final SchemaMapper<Object> schemaMapper = new SchemaMapper<>(remoteTableQuery, new MockValueMapperFactory());
        final List<List<ValueExpression>> result = schemaMapper
                .mapRow(new MockObjectNode(Map.of("testKey", new MockValueNode("testValue"))))
                .collect(Collectors.toList());
        assertAll(//
                () -> assertThat(result.size(), equalTo(1)),
                () -> assertThat(result.get(0), containsInAnyOrder(STRING_LITERAL))//
        );
    }

    @Test
    void testMapNestedTable() {
        final String nestedListKey = "topics";
        final DocumentPathExpression pathToNestedTable = new DocumentPathExpression.Builder()
                .addObjectLookup(nestedListKey).addArrayAll().build();
        final ToJsonColumnMappingDefinition columnMapping = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("test", pathToNestedTable,
                        LookupFailBehaviour.EXCEPTION));
        final TableMappingDefinition tableMapping = TableMappingDefinition
                .nestedTableBuilder("table", "table", pathToNestedTable).withColumnMappingDefinition(columnMapping)
                .build();
        final RemoteTableQuery<Object> remoteTableQuery = new RemoteTableQuery<>(tableMapping, List.of(columnMapping),
                new NoPredicate<>());
        final SchemaMapper<Object> schemaMapper = new SchemaMapper<>(remoteTableQuery, new MockValueMapperFactory());
        final List<List<ValueExpression>> result = schemaMapper
                .mapRow(new MockObjectNode(Map.of(nestedListKey,
                        new MockArrayNode(List.of(new MockValueNode("testValue"), new MockValueNode("testValue"))))))
                .collect(Collectors.toList());
        assertAll(//
                () -> assertThat(result.size(), equalTo(2)),
                () -> assertThat(result.get(0), containsInAnyOrder(STRING_LITERAL))//
        );
    }

    private static class MockValueMapperFactory implements ValueMapperFactory<Object> {

        @Override
        public AbstractValueMapper<Object> getValueMapperForColumn(final ColumnMappingDefinition column) {
            return new MockValueMapper(column);
        }
    }

    private static class MockValueMapper extends AbstractValueMapper<Object> {

        public MockValueMapper(final ColumnMappingDefinition column) {
            super(column);
        }

        @Override
        protected ValueExpression mapValue(final DocumentNode<Object> remoteProperty) {
            return STRING_LITERAL;
        }
    }
}
