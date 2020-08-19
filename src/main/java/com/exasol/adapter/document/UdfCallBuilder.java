package com.exasol.adapter.document;

import static com.exasol.sql.expression.ExpressionTerm.column;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.adapter.document.querypredicate.QueryPredicate;
import com.exasol.adapter.document.querypredicate.QueryPredicateToBooleanExpressionConverter;
import com.exasol.adapter.metadata.DataType;
import com.exasol.datatype.type.*;
import com.exasol.datatype.type.Boolean;
import com.exasol.sql.*;
import com.exasol.sql.dql.select.Select;
import com.exasol.sql.dql.select.rendering.SelectRenderer;
import com.exasol.sql.expression.BooleanExpression;
import com.exasol.sql.rendering.StringRendererConfig;
import com.exasol.utils.StringSerializer;

/**
 * This class builds push down SQL statement with a UDF call to {@link ImportDocumentData}.
 * 
 * @implNote the push down statement consists of three cascaded statements.
 * 
 *           Consider the following example:
 *
 * 
 *           SELECT COL1 FROM (
 * 
 *           SELECT UDF(PARAMS) EMITS (COL1, COL2) FROM VALUES ((v1, 1), (v2, 2), (v3, 3)) AS P1, C GROUP BY C
 * 
 *           ) WHERE COL2 = X
 *
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class UdfCallBuilder<DocumentVisitorType> {
    private static final String DOCUMENT_FETCHER_PARAMETER = "DOCUMENT_FETCHER";
    private static final String REMOTE_TABLE_QUERY_PARAMETER = "REMOTE_TABLE_QUERY";
    private static final String CONNECTION_NAME_PARAMETER = "CONNECTION_NAME";
    private static final String FRAGMENT_ID = "FRAGMENT_ID";
    private final String connectionName;
    private final String adapterName;

    /**
     * Create an instance of {@link UdfCallBuilder}.
     * 
     * @param connectionName connectionName that is passed to the UDF
     * @param adapterName    name of the adapter
     */
    public UdfCallBuilder(final String connectionName, final String adapterName) {
        this.connectionName = connectionName;
        this.adapterName = adapterName;
    }

    /**
     * Build push down SQL statement with a UDF call to {@link ImportDocumentData}.
     * 
     * @param documentFetchers document fetchers. Each document fetcher gets a row that is passed to a UDF
     * @param query            document query that is passed to the UDF
     * @return built SQL statement
     * @throws IOException if serialization of a document fetcher or the query fails
     */
    public String getUdfCallSql(final List<DocumentFetcher<DocumentVisitorType>> documentFetchers,
            final RemoteTableQuery query) throws IOException {
        final Select udfCallStatement = buildUdfCallStatement(documentFetchers, query);
        final Select pushDownSelect = wrapStatementInStatementWithPostSelectionAndProjection(query.getSelectList(),
                query.getPostSelection(), udfCallStatement);
        return renderStatement(pushDownSelect);
    }

    /**
     * Wrap the given {@code SELECT} statement in a new {@code SELECT} statement that adds the post selection as
     * {@code WHERE} clause and the projection as select {@code SELECT} clause.
     *
     * @implNote The post selection can't be applied directly to statement containing the UDF calls as Exasol does not
     *           recognize the column names correctly in the same statement.
     */
    private Select wrapStatementInStatementWithPostSelectionAndProjection(final List<ColumnMapping> selectList,
            final QueryPredicate postSelection, final Select doubleNestedSelect) {
        final String[] selectListStrings = selectList.stream().map(ColumnMapping::getExasolColumnName)
                .toArray(String[]::new);
        final Select statement = StatementFactory.getInstance().select().field(selectListStrings);
        statement.from().select(doubleNestedSelect);
        final BooleanExpression whereClause = new QueryPredicateToBooleanExpressionConverter().convert(postSelection);
        statement.where(whereClause);
        return statement;
    }

    /**
     * Build the {@code SELECT} statement that contains the call to the UDF and distributes them using a GROUP BY
     * statement.
     */
    private Select buildUdfCallStatement(final List<DocumentFetcher<DocumentVisitorType>> documentFetchers,
            final RemoteTableQuery query) throws IOException {
        final Select doubleNestedSelect = StatementFactory.getInstance().select();
        final List<Column> emitsColumns = query
                .getRequiredColumns().stream().map(column -> new Column(doubleNestedSelect,
                        column.getExasolColumnName(), convertDataType(column.getExasolDataType())))
                .collect(Collectors.toList());
        doubleNestedSelect.udf("Adapter." + ImportDocumentData.UDF_PREFIX + this.adapterName,
                new ColumnsDefinition(emitsColumns), column(DOCUMENT_FETCHER_PARAMETER),
                column(REMOTE_TABLE_QUERY_PARAMETER), column(CONNECTION_NAME_PARAMETER));
        final ValueTable valueTable = buildValueTable(documentFetchers, query, doubleNestedSelect);
        doubleNestedSelect.from().valueTableAs(valueTable, "T", DOCUMENT_FETCHER_PARAMETER,
                REMOTE_TABLE_QUERY_PARAMETER, CONNECTION_NAME_PARAMETER, FRAGMENT_ID);
        doubleNestedSelect.groupBy(column(FRAGMENT_ID));
        return doubleNestedSelect;
    }

    private ValueTable buildValueTable(final List<DocumentFetcher<DocumentVisitorType>> documentFetchers,
            final RemoteTableQuery query, final Select select) throws IOException {
        final ValueTable valueTable = new ValueTable(select);
        int rowCounter = 0;
        for (final DocumentFetcher<DocumentVisitorType> documentFetcher : documentFetchers) {
            final String serializedDocumentFetcher = StringSerializer.serializeToString(documentFetcher);
            final String serializedRemoteTableQuery = StringSerializer.serializeToString(query);
            final ValueTableRow row = ValueTableRow.builder(select).add(serializedDocumentFetcher)
                    .add(serializedRemoteTableQuery) //
                    .add(this.connectionName) //
                    .add(rowCounter) //
                    .build();
            valueTable.appendRow(row);
            ++rowCounter;
        }
        return valueTable;
    }

    private com.exasol.datatype.type.DataType convertDataType(final DataType adapterDataType) {
        switch (adapterDataType.getExaDataType()) {
        case DECIMAL:
            return new Decimal(adapterDataType.getPrecision(), adapterDataType.getScale());
        case DOUBLE:
            return new DoublePrecision();
        case VARCHAR:
            return new Varchar(adapterDataType.getSize());
        case CHAR:
            return new Char(adapterDataType.getSize());
        case DATE:
            return new Date();
        case TIMESTAMP:
            return adapterDataType.isWithLocalTimezone() ? new TimestampWithLocalTimezone() : new Timestamp();
        case BOOLEAN:
            return new Boolean();
        default:
            throw new UnsupportedOperationException("This DataType has no corresponding type.");
        }
    }

    private String renderStatement(final Select pushDownSelect) {
        final StringRendererConfig config = StringRendererConfig.builder().quoteIdentifiers(true).build();
        final SelectRenderer renderer = new SelectRenderer(config);
        pushDownSelect.accept(renderer);
        return renderer.render();
    }
}
