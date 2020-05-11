package com.exasol.adapter.dynamodb.queryrunner;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.mapping.ColumnMappingDefinition;
import com.exasol.adapter.dynamodb.mapping.JsonMappingFactory;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.dynamodb.mapping.TableMappingDefinition;
import com.exasol.adapter.dynamodb.remotetablequery.ColumnLiteralComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.ComparisonPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.NoPredicate;
import com.exasol.adapter.dynamodb.remotetablequery.RemoteTableQuery;

public class BasicMappingSetup {

    public final static String PRIMARY_KEY_NAME = "isbn";
    public final static String INDEX_NAME = "publisherIndex";
    public final static String INDEX_PARTITION_KEY = "publisher";
    public final static String INDEX_SORT_KEY = "price";

    public TableMappingDefinition tableMapping;

    public BasicMappingSetup() throws IOException, AdapterException {
        this.tableMapping = new JsonMappingFactory(MappingTestFiles.BASIC_MAPPING_FILE).getSchemaMapping()
                .getTableMappings().get(0);
    }

    @NotNull
    public RemoteTableQuery<DynamodbNodeVisitor> getSelectAllQuery() {
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), new NoPredicate<>());
    }

    /**
     * Creates a query that should fetch one item based on the primary key {@code isbn}.
     * 
     * @param isbn primary keys value
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForIsbn(final String isbn) {
        final ColumnMappingDefinition isbnColumn = this.tableMapping.getColumns().stream()
                .filter(column -> column.getExasolColumnName().equals("ISBN")).findAny().get();
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> selection = new ColumnLiteralComparisonPredicate<>(
                ComparisonPredicate.Operator.EQUAL, isbnColumn, new DynamodbString(isbn));
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Creates a query that should return all books of a given publisher. For the publisher there is an index that can
     * be used so this query can be answered using a {@code Query} operation.
     * 
     * @param publisher name of the publisher to query
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForPublisher(final String publisher) {
        final ColumnMappingDefinition publisherColumn = this.tableMapping.getColumns().stream()
                .filter(column -> column.getExasolColumnName().equals("PUBLISHER")).findAny().get();
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> selection = new ColumnLiteralComparisonPredicate<>(
                ComparisonPredicate.Operator.EQUAL, publisherColumn, new DynamodbString(publisher));
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }
}
