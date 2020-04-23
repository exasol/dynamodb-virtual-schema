package com.exasol.adapter.dynamodb.queryplan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.DynamodbValueMapperFactory;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.ValueExpression;

public class RowMapperTest {
    // TODO remove DynamoDB dependency
    @Test
    public void testMapRow() {
        final ToJsonColumnMappingDefinition mappingDefinition = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters("test",
                        new DocumentPathExpression.Builder().build(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
        final DocumentQuery<DynamodbNodeVisitor> documentQuery = new DocumentQuery<>(null, List.of(mappingDefinition),
                new AndPredicate<DynamodbNodeVisitor>(Collections.emptyList()));
        final List<ValueExpression> exasolRow = new RowMapper<DynamodbNodeVisitor>(documentQuery,
                new DynamodbValueMapperFactory())
                        .mapRow(new DynamodbMap(Map.of("testKey", AttributeValueQuickCreator.forString("testValue"))));
        assertThat(exasolRow.get(0).toString(), equalTo("{\"testKey\":\"testValue\"}"));
    }
}
