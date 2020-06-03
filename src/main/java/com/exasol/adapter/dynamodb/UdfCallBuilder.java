package com.exasol.adapter.dynamodb;

import static com.exasol.sql.expression.ExpressionTerm.column;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.exasol.adapter.dynamodb.documentfetcher.DocumentFetcher;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;
import com.exasol.adapter.metadata.DataType;
import com.exasol.datatype.type.*;
import com.exasol.datatype.type.Boolean;
import com.exasol.sql.*;
import com.exasol.sql.dql.select.Select;
import com.exasol.sql.dql.select.rendering.SelectRenderer;
import com.exasol.sql.rendering.StringRendererConfig;
import com.exasol.utils.StringSerializer;

/**
 * This class builds push down SQL statement with a UDF call to {@link ImportDocumentData}.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class UdfCallBuilder<DocumentVisitorType> {

    /**
     * Build push down SQL statement with a UDF call to {@link ImportDocumentData}.
     * 
     * @param documentFetchers document fetchers. Each document fetcher gets a row that is passed to a UDF
     * @param query            document query that is passed to the UDF
     * @param connectionName   connectionName that is passed to the UDF
     * @return built SQL statement
     * @throws IOException if serialization of a documentFetcher or the query fails
     */
    public String getUdfCallSql(final List<DocumentFetcher<DocumentVisitorType>> documentFetchers,
            final RemoteTableQuery<DocumentVisitorType> query, final String connectionName) throws IOException {
        final String documentFetcherParameter = "DOCUMENT_FETCHER";
        final String remoteTableQueryParameter = "REMOTE_TABLE_QUERY";
        final String connectionNameParameter = "CONNECTION_NAME";
        final StringRendererConfig config = StringRendererConfig.builder().quoteIdentifiers(true).build();
        final SelectRenderer renderer = new SelectRenderer(config);
        final Select select = StatementFactory.getInstance().select();
        final List<Column> emitsColumns = query.getSelectList().stream().map(
                column -> new Column(select, column.getExasolColumnName(), convertDataType(column.getExasolDataType())))
                .collect(Collectors.toList());
        // TODO UDF name as parameter
        select.udf("Adapter." + ImportDocumentData.class.getSimpleName(), new ColumnsDefinition(emitsColumns),
                column(documentFetcherParameter), column(remoteTableQueryParameter), column(connectionNameParameter));
        final ValueTable valueTable = buildValueTable(documentFetchers, query, connectionName, select);
        select.from().valueTable(valueTable);
        select.accept(renderer);
        // TODO refactor when https://github.com/exasol/sql-statement-builder/issues/76 is fixed
        return renderer.render() + " AS T(" + documentFetcherParameter + ", " + remoteTableQueryParameter + ", "
                + connectionNameParameter + ")";
    }

    private ValueTable buildValueTable(final List<DocumentFetcher<DocumentVisitorType>> documentFetchers,
            final RemoteTableQuery<DocumentVisitorType> query, final String connectionName, final Select select)
            throws IOException {
        final ValueTable valueTable = new ValueTable(select);
        for (final DocumentFetcher<DocumentVisitorType> documentFetcher : documentFetchers) {
            final String serializedDocumentFetcher = StringSerializer.serializeToString(documentFetcher);
            final String serializedRemoteTableQuery = StringSerializer.serializeToString(query);
            final ValueTableRow row = ValueTableRow.builder(select).add(serializedDocumentFetcher)
                    .add(serializedRemoteTableQuery).add(connectionName).build();
            valueTable.appendRow(row);
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
}
