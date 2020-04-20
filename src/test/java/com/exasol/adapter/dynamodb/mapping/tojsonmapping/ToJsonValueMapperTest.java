package com.exasol.adapter.dynamodb.mapping.tojsonmapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.dynamodb.attributevalue.AttributeValueQuickCreator;
import com.exasol.dynamodb.resultwalker.IdentityDynamodbResultWalker;
import com.exasol.sql.expression.ValueExpression;

public class ToJsonValueMapperTest {
    private static final String DEST_COLUMN = "destColumn";

    @Test
    void testConvertRowBasic() {
        final ToJsonColumnMappingDefinition toStringColumnMappingDefinition = new ToJsonColumnMappingDefinition(
                new AbstractColumnMappingDefinition.ConstructorParameters(DEST_COLUMN,
                        new IdentityDynamodbResultWalker(),
                        AbstractColumnMappingDefinition.LookupFailBehaviour.EXCEPTION));
        final Map<String, AttributeValue> rootAttributeValue = Map.of("key",
                AttributeValueQuickCreator.forString("value"));
        final ValueExpression exasolCellValue = new ToJsonValueMapper(toStringColumnMappingDefinition)
                .mapRow(rootAttributeValue);
        assertThat(exasolCellValue.toString(), equalTo("{\"key\":\"value\"}"));
    }
}
