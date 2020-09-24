package com.exasol.adapter.document;

import static com.exasol.adapter.document.UdfEntryPoint.*;
import static com.exasol.adapter.document.dynamodb.DynamodbAdapter.ADAPTER_NAME;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.exasol.bucketfs.BucketAccessException;

/**
 * DynamoDB specific test database builder.
 */
public class DynamodbVsExasolTestDatabaseBuilder extends ExasolTestDatabaseBuilder {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "document-virtual-schema-dist-2.0.0-dynamodb-2.0.0.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);

    public DynamodbVsExasolTestDatabaseBuilder(final ExasolTestInterface testInterface)
            throws SQLException, IOException {
        super(testInterface);
    }

    /**
     * Uploads the dynamodb adapter jar to the exasol test container.
     */
    public void uploadDynamodbAdapterJar() throws InterruptedException, BucketAccessException, TimeoutException {
        this.testInterface.uploadFileToBucketfs(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        uploadJacocoAgent();
    }

    public void createUdf() throws SQLException {
        final StringBuilder statementBuilder = new StringBuilder("CREATE OR REPLACE JAVA SET SCRIPT " + ADAPTER_SCHEMA
                + "." + UDF_PREFIX + ADAPTER_NAME + "(" + PARAMETER_DATA_LOADER + " VARCHAR(2000000), "
                + PARAMETER_REMOTE_TABLE_QUERY
                + " VARCHAR(2000000), " + PARAMETER_CONNECTION_NAME + " VARCHAR(500)) EMITS(...) AS\n");
        statementBuilder.append(getDebuggerOptions(true));
        statementBuilder.append("    %scriptclass " + UdfEntryPoint.class.getName() + ";\n");
        statementBuilder.append("    %jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n");
        statementBuilder.append("/");
        final String sql = statementBuilder.toString();
        LOGGER.info(sql);
        this.getStatement().execute(sql);
    }

    /**
     * Runs {@code CREATE OR REPLACE JAVA ADAPTER SCRIPT} on the Exasol test container with the DynamoDB Virtual Schema
     * adapter jar.
     *
     * @throws SQLException on SQL error
     */
    public void createAdapterScript() throws SQLException {
        setScriptOutputAddress();
        dropSchema(ADAPTER_SCHEMA);
        createSchema(ADAPTER_SCHEMA);
        final StringBuilder statementBuilder = new StringBuilder(
                "CREATE OR REPLACE JAVA ADAPTER SCRIPT " + ADAPTER_SCHEMA + "." + DYNAMODB_ADAPTER + " AS\n");
        statementBuilder.append(getDebuggerOptions(false));
        // noinspection SpellCheckingInspection
        statementBuilder.append("    %scriptclass com.exasol.adapter.RequestDispatcher;\n");
        statementBuilder.append("    %jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n");
        statementBuilder.append("/");
        final String sql = statementBuilder.toString();
        LOGGER.info(sql);
        this.getStatement().execute(sql);
    }
}
