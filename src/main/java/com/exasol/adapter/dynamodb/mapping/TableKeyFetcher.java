package com.exasol.adapter.dynamodb.mapping;

import java.util.List;

/**
 * This interfaces tries to fetches a global key from for a remote table.
 */
public interface TableKeyFetcher {

    /**
     * This method tries to build a global key from given column mappings.
     * 
     * @param tableName     name of the remote table
     * @param mappedColumns available column mappings
     * @return global key columns
     * @throws NoKeyFoundException if no fitting key was found
     */
    public List<ColumnMapping> fetchKeyForTable(String tableName, List<ColumnMapping> mappedColumns)
            throws NoKeyFoundException;

    /**
     * {@link TableKeyFetcher}s throw this exception if they did not find a fitting key.
     */
    public class NoKeyFoundException extends Exception {
        private static final long serialVersionUID = 7924854713502228769L;
    }
}
