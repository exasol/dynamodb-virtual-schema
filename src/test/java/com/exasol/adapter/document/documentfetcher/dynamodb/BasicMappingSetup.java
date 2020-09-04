package com.exasol.adapter.document.documentfetcher.dynamodb;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.document.mapping.ColumnMapping;
import com.exasol.adapter.document.mapping.MappingTestFiles;
import com.exasol.adapter.document.mapping.TableMapping;
import com.exasol.adapter.document.mapping.reader.JsonSchemaMappingReader;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.adapter.document.querypredicate.*;
import com.exasol.adapter.sql.SqlLiteralDouble;
import com.exasol.adapter.sql.SqlLiteralString;

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

    public BasicMappingSetup(final Path tempDir) throws IOException, AdapterException {
        this.tableMapping = new JsonSchemaMappingReader(
                MappingTestFiles.getMappingAsFile(MappingTestFiles.BASIC_MAPPING, tempDir), null).getSchemaMapping()
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

    public RemoteTableQuery getSelectAllQuery() {
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), new NoPredicate(),
                new NoPredicate());
    }

    public RemoteTableQuery getSelectAllQueryWithNameColumnProjected() {
        return new RemoteTableQuery(this.tableMapping, List.of(this.nameColumn), new NoPredicate(), new NoPredicate());
    }

    /**
     * Create a query that should fetch one item based on the primary key {@code isbn}.
     * 
     * @param isbn primary keys value
     * @return query
     */
    public RemoteTableQuery getQueryForIsbn(final String isbn) {
        final ColumnLiteralComparisonPredicate selection = new ColumnLiteralComparisonPredicate(
                AbstractComparisonPredicate.Operator.EQUAL, this.isbnColumn, new SqlLiteralString(isbn));
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE NOT(ISBN = :isbn)}
     * 
     * @param isbn isbn to query
     * @return query
     */
    public RemoteTableQuery getQueryForNotIsbn(final String isbn) {
        final QueryPredicate selection = new NotPredicate(new ColumnLiteralComparisonPredicate(
                AbstractComparisonPredicate.Operator.EQUAL, this.isbnColumn, new SqlLiteralString(isbn)));
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }

    /**
     * Create a query that should return all books of a given publisher. For the publisher there is an index that can be
     * used so this query can be answered using a {@code Query} operation.
     * 
     * @param publisher name of the publisher to query
     * @return query
     */
    public RemoteTableQuery getQueryForPublisher(final String publisher) {
        final ColumnLiteralComparisonPredicate selection = new ColumnLiteralComparisonPredicate(
                AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn, new SqlLiteralString(publisher));
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE price > :price}
     *
     * @param price to filter
     * @return query
     */
    public RemoteTableQuery getQueryForMinPrice(final double price) {
        final QueryPredicate selection = new ColumnLiteralComparisonPredicate(
                AbstractComparisonPredicate.Operator.GREATER, this.priceColumn, new SqlLiteralDouble(price));
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE price > :price AND publisher = :publisher}
     *
     * @param price     to selection
     * @param publisher for selection
     * @return query
     */
    public RemoteTableQuery getQueryForMinPriceAndPublisher(final double price, final String publisher) {
        final QueryPredicate selection = new LogicalOperator(Set.of(
                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.GREATER, this.priceColumn,
                        new SqlLiteralDouble(price)),
                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn,
                        new SqlLiteralString(publisher))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE NOT(price > :price) AND publisher = :publisher}
     *
     * @param price     to selection
     * @param publisher for selection
     * @return query
     */
    public RemoteTableQuery getQueryForMaxPriceAndPublisher(final double price, final String publisher) {
        final QueryPredicate selection = new LogicalOperator(Set.of(
                new NotPredicate(new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.GREATER,
                        this.priceColumn, new SqlLiteralDouble(price))),
                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn,
                        new SqlLiteralString(publisher))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }

    /**
     * Create a query representing: {@code SELECT * FROM BOOKS WHERE name = :name AND publisher = :publisher}
     *
     * @param name      for selection
     * @param publisher to selection
     * @return query
     */
    public RemoteTableQuery getQueryForNameAndPublisher(final String name, final String publisher) {
        final QueryPredicate selection = new LogicalOperator(Set.of(
                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL, this.nameColumn,
                        new SqlLiteralString(String.valueOf(name))),
                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn,
                        new SqlLiteralString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
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
    public RemoteTableQuery getQueryForTwoNamesAndPublisher(final String name1, final String name2,
            final String publisher) {
        final QueryPredicate selection = new LogicalOperator(Set.of(
                new LogicalOperator(
                        Set.of(new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                                this.nameColumn, new SqlLiteralString(String.valueOf(name1))),
                                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                                        this.nameColumn, new SqlLiteralString(String.valueOf(name2)))),
                        LogicalOperator.Operator.OR),
                new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL, this.publisherColumn,
                        new SqlLiteralString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
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
    public RemoteTableQuery getQueryForPriceAndPublisherAndIsbn(final double price, final String publisher,
            final String isbn) {
        final QueryPredicate selection = new LogicalOperator(
                Set.of(new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                        this.priceColumn, new SqlLiteralDouble(price)),
                        new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                                this.isbnColumn, new SqlLiteralString(String.valueOf(isbn))),
                        new ColumnLiteralComparisonPredicate(AbstractComparisonPredicate.Operator.EQUAL,
                                this.publisherColumn, new SqlLiteralString(String.valueOf(publisher)))),
                LogicalOperator.Operator.AND);
        return new RemoteTableQuery(this.tableMapping, this.tableMapping.getColumns(), selection, new NoPredicate());
    }
}
