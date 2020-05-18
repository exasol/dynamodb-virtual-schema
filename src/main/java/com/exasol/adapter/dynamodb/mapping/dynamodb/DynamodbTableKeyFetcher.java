package com.exasol.adapter.dynamodb.mapping.dynamodb;

import java.util.List;

import com.exasol.adapter.dynamodb.documentpath.DocumentPathExpression;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadata;
import com.exasol.adapter.dynamodb.dynamodbmetadata.DynamodbTableMetadataFactory;
import com.exasol.adapter.dynamodb.mapping.ColumnMapping;
import com.exasol.adapter.dynamodb.mapping.PropertyToColumnMapping;
import com.exasol.adapter.dynamodb.mapping.TableKeyFetcher;

/**
 * This class selects the unique columns for a DynamoDB table using metadata fetched from DynamoDB.
 */
public class DynamodbTableKeyFetcher implements TableKeyFetcher {
    private final DynamodbTableMetadataFactory tableMetadataFactory;

    /**
     * Creates an instance of {@link DynamodbTableKeyFetcher}.
     * 
     * @param tableMetadataFactory metadata factory
     */
    public DynamodbTableKeyFetcher(final DynamodbTableMetadataFactory tableMetadataFactory) {
        this.tableMetadataFactory = tableMetadataFactory;
    }

    @Override
    public List<ColumnMapping> fetchKeyForTable(final String tableName, final List<ColumnMapping> mappedColumns)
            throws NoKeyFoundException {
        final DynamodbTableMetadata tableMetadata = this.tableMetadataFactory.buildMetadataForTable(tableName);
        final ColumnMapping partitionKeyColumn = findColumnMappingByName(mappedColumns,
                tableMetadata.getPrimaryIndex().getPartitionKey());
        if (tableMetadata.getPrimaryIndex().hasSortKey()) {
            final ColumnMapping sortKeyColumn = findColumnMappingByName(mappedColumns,
                    tableMetadata.getPrimaryIndex().getSortKey());
            return List.of(partitionKeyColumn, sortKeyColumn);
        } else {
            return List.of(partitionKeyColumn);
        }
    }

    private ColumnMapping findColumnMappingByName(final List<ColumnMapping> allColumns, final String propertyName)
            throws NoKeyFoundException {
        for (final ColumnMapping column : allColumns) {
            if (!(column instanceof PropertyToColumnMapping)) {
                continue;
            }
            final PropertyToColumnMapping propertyToColumnMapping = (PropertyToColumnMapping) column;
            final DocumentPathExpression propertiesPath = new DocumentPathExpression.Builder()
                    .addObjectLookup(propertyName).build();
            if (propertyToColumnMapping.getPathToSourceProperty().equals(propertiesPath)) {
                return propertyToColumnMapping;
            }
        }
        throw new NoKeyFoundException();
    }
}
