package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

import java.io.IOException;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNodeVisitor;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbNumber;
import com.exasol.adapter.dynamodb.documentnode.dynamodb.DynamodbString;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.JsonSchemaMappingReader;
import com.exasol.adapter.dynamodb.mapping.MappingTestFiles;
import com.exasol.adapter.dynamodb.mapping.TableMapping;
import com.exasol.adapter.dynamodb.remotetablequery.*;

public class BasicMappingSetup {

    public final static String PRIMARY_KEY_NAME = "isbn";
    public final static String INDEX_NAME = "publisherIndex";
    public final static String INDEX_PARTITION_KEY = "publisher";
    public final static String INDEX_SORT_KEY = "price";
    public final TableMapping tableMapping;
    private final ColumnMapping publisherColumn;
    private final ColumnMapping priceColumn;
    private final ColumnMapping nameColumn;
    private final ColumnMapping isbnColumn;

    public BasicMappingSetup() throws IOException, AdapterException {
        this.tableMapping = new JsonSchemaMappingReader(MappingTestFiles.BASIC_MAPPING_FILE, null).getSchemaMapping()
                .getTableMappings().get(0);
        this.publisherColumn = this.tableMapping.getColumns().stream()
                .filter(column -> column.getExasolColumnName().equals("PUBLISHER")).findAny().get();
        this.priceColumn = this.tableMapping.getColumns().stream()
                .filter(column -> column.getExasolColumnName().equals("PRICE")).findAny().get();
        this.nameColumn = this.tableMapping.getColumns().stream()
                .filter(column -> column.getExasolColumnName().equals("NAME")).findAny().get();
        this.isbnColumn = this.tableMapping.getColumns().stream()
                .filter(column -> column.getExasolColumnName().equals("ISBN")).findAny().get();
    }

    public RemoteTableQuery<DynamodbNodeVisitor> getSelectAllQuery() {
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), new NoPredicate<>());
    }

    /**
     * Create a query that should fetch one item based on the primary key {@code isbn}.
     * 
     * @param isbn primary keys value
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForIsbn(final String isbn) {
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> selection = new ColumnLiteralComparisonPredicate<>(
                AbstractComparisonPredicate.Operator.EQUAL, this.isbnColumn, new DynamodbString(isbn));
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE NOT(ISBN = :isbn)}
     * 
     * @param isbn isbn to query
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForNotIsbn(final String isbn) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new NotPredicate<>(new ColumnLiteralComparisonPredicate<>(
                AbstractComparisonPredicate.Operator.EQUAL, this.isbnColumn, new DynamodbString(isbn)));
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query that should return all books of a given publisher. For the publisher there is an index that can be
     * used so this query can be answered using a {@code Query} operation.
     * 
     * @param publisher name of the publisher to query
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForPublisher(final String publisher) {
        final ColumnLiteralComparisonPredicate<DynamodbNodeVisitor> selection = new ColumnLiteralComparisonPredicate<>(
                AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn, new DynamodbString(publisher));
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE price > :price}
     *
     * @param price to filter
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForMinPrice(final double price) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new ColumnLiteralComparisonPredicate<>(
                AbstractComparisonPredicate.Operator.GREATER, this.priceColumn,
                new DynamodbNumber(String.valueOf(price)));
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE price > :price AND publisher = :publisher}
     *
     * @param price     to selection
     * @param publisher for selection
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForMinPriceAndPublisher(final double price,
            final String publisher) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new LogicalOperator<>(
                List.of(new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.GREATER,
                        this.priceColumn, new DynamodbNumber(String.valueOf(price))),
                        new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                                this.publisherColumn, new DynamodbString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE NOT(price > :price) AND publisher = :publisher}
     *
     * @param price     to selection
     * @param publisher for selection
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForMaxPriceAndPublisher(final double price,
            final String publisher) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new LogicalOperator<>(List.of(
                new NotPredicate<>(new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.GREATER,
                        this.priceColumn, new DynamodbNumber(String.valueOf(price)))),
                new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn,
                        new DynamodbString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE name = :name AND publisher = :publisher}
     *
     * @param name      for selection
     * @param publisher to selection
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForNameAndPublisher(final String name,
            final String publisher) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new LogicalOperator<>(
                List.of(new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                        this.nameColumn, new DynamodbString(String.valueOf(name))),
                        new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                                this.publisherColumn, new DynamodbString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing:
     * {@code SELECT * FROM BOOKS WHERE (name = :name1 OR name = :name2) AND publisher = :publisher}
     *
     * @param name1     for selection
     * @param name2     for selection
     * @param publisher for selection
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForTwoNamesAndPublisher(final String name1, final String name2,
            final String publisher) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new LogicalOperator<>(List.of(
                new LogicalOperator<>(
                        List.of(new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                                this.nameColumn, new DynamodbString(String.valueOf(name1))),
                                new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                                        this.nameColumn, new DynamodbString(String.valueOf(name2)))),
                        LogicalOperator.Operator.OR),
                new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn,
                        new DynamodbString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

    /**
     * Create a query representing:
     * {@code SELECT * FROM BOOKS WHERE price = :price AND publisher = :publisher AND ISBN = :isbn}
     *
     * @param price     for selection
     * @param publisher for selection
     * @param isbn      for selection
     * @return query
     */
    public RemoteTableQuery<DynamodbNodeVisitor> getQueryForPriceAndPublisherAndIsbn(final String price,
            final String publisher, final String isbn) {
        final QueryPredicate<DynamodbNodeVisitor> selection = new LogicalOperator<>(
                List.of(new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                        this.priceColumn, new DynamodbNumber(String.valueOf(price))),
                        new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                                this.isbnColumn, new DynamodbString(String.valueOf(isbn))),
                        new ColumnLiteralComparisonPredicate<>(AbstractComparisonPredicate.Operator.EQUAL,
                                this.publisherColumn, new DynamodbString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery<>(this.tableMapping, this.tableMapping.getColumns(), selection);
    }

}
