package com.exasol.adapter.dynamodb.mapping.tojsonmapping.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbMap;
import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.tojsonmapping.ToJsonColumnMappingDefinition;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.sql.expression.ValueExpression;

public class DynamodbToJsonValueMapperTest {
    private static final String DEST_COLUMN = "destColumn";

    @Test
    void testConvertRowBasic() {
        final ToJsonColumnMappingDefinition toStringColumnMappingDefinition = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters(DEST_COLUMN,
                        new DocumentPathExpression.Builder().build(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
        final DynamodbMap testData = new DynamodbMap(Map.of("key", AttributeValueQuickCreator.forString("value")));
        final ValueExpression exasolCellValue = new DynamodbToJsonValueMapper(toStringColumnMappingDefinition)
                .mapRow(testData);
        assertThat(exasolCellValue.toString(), equalTo("{\"key\":\"value\"}"));
    }
}
