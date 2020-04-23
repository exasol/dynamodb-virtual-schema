package com.exasol.adapter.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.mapping.AbstractColumnMappingDefinition;
import com.exasol.adapter.dynamodb.queryplan.DocumentQueryMappingInterface;
import com.exasol.sql.StatementFactory;
import com.exasol.sql.ValueTable;
import com.exasol.sql.ValueTableRow;
import com.exasol.sql.dql.select.Select;
import com.exasol.sql.dql.select.rendering.SelectRenderer;
import com.exasol.sql.expression.BooleanLiteral;
import com.exasol.sql.expression.ValueExpression;
import com.exasol.sql.rendering.StringRendererConfig;

/**
 * Converts a list of Exasol {@link ValueExpression}s into a {@code SELECT FROM VALUES} statement.
 */
public class ValueExpressionsToSqlSelectFromValuesConverter {

    public String convert(final DocumentQueryMappingInterface tableStructure, final List<List<ValueExpression>> rows) {
        final StringRendererConfig config = StringRendererConfig.builder().quoteIdentifiers(true).build();
        final SelectRenderer renderer = new SelectRenderer(config);
        convertToSelect(tableStructure, rows).accept(renderer);
        return renderer.render();
    }

    private Select convertToSelect(final DocumentQueryMappingInterface tableStructure,
            final List<List<ValueExpression>> rows) {
        final Select select = StatementFactory.getInstance().select();
        final ValueTable valueTable = new ValueTable(select);

        if (!rows.isEmpty()) {
            for (final List<ValueExpression> row : rows) {
                final ValueTableRow valueTableRow = ValueTableRow.builder(select).add(row).build();
                valueTable.appendRow(valueTableRow);
            }
        } else {
            final ValueTableRow defaultValueRow = ValueTableRow.builder(select).add(getDefaultValueRow(tableStructure))
                    .build();
            valueTable.appendRow(defaultValueRow);
            select.where(BooleanLiteral.of(false));
        }
        select.all().from().valueTable(valueTable);
        return select;
    }

    private List<ValueExpression> getDefaultValueRow(final DocumentQueryMappingInterface tableStructure) {
        return tableStructure.getSelectList().stream().map(AbstractColumnMappingDefinition::getExasolDefaultValue)
                .collect(Collectors.toList());
    }
}
