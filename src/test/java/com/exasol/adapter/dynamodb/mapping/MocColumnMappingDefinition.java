package com.exasol.adapter.dynamodb.mapping;

import com.exasol.adapter.metadata.DataType;
import com.exasol.dynamodb.resultwalker.AbstractDynamodbResultWalker;
import com.exasol.sql.expression.StringLiteral;
import com.exasol.sql.expression.ValueExpression;

public class MocColumnMappingDefinition extends AbstractColumnMappingDefinition {
	public MocColumnMappingDefinition(final String destinationName, final AbstractDynamodbResultWalker resultWalker,
			final LookupFailBehaviour lookupFailBehaviour) {
		super(destinationName, resultWalker, lookupFailBehaviour);
	}

	@Override
	public DataType getDestinationDataType() {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public ValueExpression getDestinationDefaultValue() {
		return StringLiteral.of("default");
	}

	@Override
	public boolean isDestinationNullable() {
		return false;
	}

	@Override
	public void accept(final ColumnMappingDefinitionVisitor visitor) {
		throw new UnsupportedOperationException("not implemented");
	}
}
