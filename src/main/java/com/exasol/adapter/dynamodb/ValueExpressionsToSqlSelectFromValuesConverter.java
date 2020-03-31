package com.exasol.adapter.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.queryresult.QueryResultTable;
import com.exasol.sql.StatementFactory;
import com.exasol.sql.ValueTable;
import com.exasol.sql.ValueTableRow;
import com.exasol.sql.dql.select.Select;
import com.exasol.sql.dql.select.rendering.SelectRenderer;
import com.exasol.sql.expression.BooleanLiteral;
import com.exasol.sql.expression.ValueExpression;
import com.exasol.sql.rendering.StringRendererConfig;

/**
 * This implementation of {@link ValueExpressionsToSqlConverter} converts a
 * DynamoDB result into an {@code SELECT FROM VALUES} statement.
 */
public class ValueExpressionsToSqlSelectFromValuesConverter implements ValueExpressionsToSqlConverter {

	@Override
	public String convert(final QueryResultTable tableStructure, final List<List<ValueExpression>> rows) {
		final StringRendererConfig config = StringRendererConfig.builder().quoteIdentifiers(true).build();
		final SelectRenderer renderer = new SelectRenderer(config);
		convertToSelect(tableStructure, rows).accept(renderer);
		return renderer.render();
	}

	private Select convertToSelect(final QueryResultTable tableStructure, final List<List<ValueExpression>> rows) {
		final Select select = StatementFactory.getInstance().select();
		final ValueTable valueTable = new ValueTable(select);
		for (final List<ValueExpression> row : rows) {
			final ValueTableRow valueTableRow = ValueTableRow.builder(select).add(row).build();
			valueTable.appendRow(valueTableRow);
		}
		if (rows.isEmpty()) {
			final ValueTableRow defaultValueRow = ValueTableRow.builder(select).add(getDefaultValueRow(tableStructure))
					.build();
			valueTable.appendRow(defaultValueRow);
			select.where(BooleanLiteral.of(false));
		}
		select.all().from().valueTable(valueTable);
		return select;
	}

	private List<ValueExpression> getDefaultValueRow(final QueryResultTable tableStructure) {
		return tableStructure.getColumns().stream()
				.map(column -> column.getColumnMapping().getDestinationDefaultValue()).collect(Collectors.toList());
	}
}