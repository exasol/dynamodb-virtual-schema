package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.bucketfs.BucketAccessException;

/**
 * This class contains helper methods for creating an integration test setup with the exasol database.
 */
public class ExasolTestDatabaseBuilder {
    public static final String ADAPTER_SCHEMA = "ADAPTER";
    public static final String DYNAMODB_ADAPTER = "DYNAMODB_ADAPTER";
    public static final String PROFILING_AGENT_FILE_NAME = "liblagent.so";
    private static final Logger LOGGER = LoggerFactory.getLogger(TestcontainerExasolTestInterface.class);
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "dynamodb-virtual-schemas-adapter-dist-0.4.0.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    private static final String JACOCO_JAR_NAME = "org.jacoco.agent-runtime.jar";
    private static final Path PATH_TO_JACOCO_JAR = Path.of("target", "jacoco-agent", JACOCO_JAR_NAME);
    private static final String LOGGER_PORT = "3000";
    private static final int SCRIPT_OUTPUT_PORT = 3001;
    private static final String DEBUGGER_PORT = "8000";
    private final ExasolTestInterface testInterface;
    private final Statement statement;

    public ExasolTestDatabaseBuilder(final ExasolTestInterface testInterface) throws SQLException, IOException {
        this.testInterface = testInterface;
        this.statement = this.testInterface.getConnection().createStatement();
    }

    /**
     * @return SQL Statement for running jdbc queries on exasol test container given in constructor
     */
    public Statement getStatement() {
        return this.statement;
    }

    /**
     * Create a schema on the exasol test container.
     *
     * @param schemaName name of the schema to create
     * @throws SQLException on SQL error
     */
    public void createSchema(final String schemaName) throws SQLException {
        this.getStatement().execute("CREATE SCHEMA " + schemaName);
    }

    /**
     * Drops a schema
     *
     * @param schemaName name of the schema to drop
     * @throws SQLException on SQL error
     */
    public void dropSchema(final String schemaName) throws SQLException {
        this.getStatement().execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
    }

    /**
     * Runs {@code CREATE CONNECTION} on the exasol test container.
     *
     * @param name name of the connection to create
     * @param to   uri
     * @param user user for the connection
     * @param pass password for the connection
     * @throws SQLException on SQL error
     */
    public void createConnection(final String name, final String to, final String user, final String pass)
            throws SQLException {
        this.getStatement().execute(
                "CREATE CONNECTION " + name + " TO '" + to + "' USER '" + user + "' IDENTIFIED BY '" + pass + "';");
    }

    public void dropConnection(final String name) throws SQLException {
        this.getStatement().execute("DROP CONNECTION IF EXISTS " + name);
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
        addDebuggerOptions(statementBuilder, false);
        // noinspection SpellCheckingInspection
        statementBuilder.append("    %scriptclass com.exasol.adapter.RequestDispatcher;\n");
        statementBuilder.append("    %jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n");
        statementBuilder.append("/");
        final String sql = statementBuilder.toString();
        LOGGER.info(sql);
        this.statement.execute(sql);
    }

    private void addDebuggerOptions(final StringBuilder statementBuilder, final boolean isUdf) {
        final String hostIp = this.testInterface.getTestHostIpAddress();
        final StringBuilder jvmOptions = new StringBuilder();
        if (hostIp != null) {
            jvmOptions.append("-javaagent:/buckets/bfsdefault/default/").append(JACOCO_JAR_NAME)
                    .append("=output=tcpclient,address=").append(hostIp).append(",port=3002");
            if ((isUdf && isUdfDebuggingEnabled()) || (!isUdf && isVirtualSchemaDebuggingEnabled())) {
                // noinspection SpellCheckingInspection
                jvmOptions.append(" -agentlib:jdwp=transport=dt_socket,server=n,address=").append(hostIp).append(":")
                        .append(DEBUGGER_PORT).append(",suspend=y");
            }
        }
        if (isProfilingEnabled()) {
            jvmOptions.append(" -agentpath:/buckets/bfsdefault/default/" + PROFILING_AGENT_FILE_NAME
                    + "=interval=7,logPath=/tmp/profile.hpl");
        }
        if (!jvmOptions.toString().isEmpty()) {
            statementBuilder.append("  %jvmoption ").append(jvmOptions).append(";\n");
        }
    }

    public void createUdf() throws SQLException {
        final StringBuilder statementBuilder = new StringBuilder("CREATE OR REPLACE JAVA SET SCRIPT ")
                .append(ADAPTER_SCHEMA).append(".").append(ImportDocumentData.UDF_NAME).append("(")
                .append(AbstractUdf.PARAMETER_DOCUMENT_FETCHER).append(" VARCHAR(2000000), ")
                .append(AbstractUdf.PARAMETER_REMOTE_TABLE_QUERY).append(" VARCHAR(2000000), ")
                .append(AbstractUdf.PARAMETER_CONNECTION_NAME).append(" VARCHAR(500)) EMITS(...) AS\n");
        addDebuggerOptions(statementBuilder, true);
        statementBuilder.append("    %scriptclass ").append(ImportDocumentData.class.getName()).append(";\n");
        statementBuilder.append("    %jar /buckets/bfsdefault/default/").append(VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION)
                .append(";\n");
        statementBuilder.append("/");
        final String sql = statementBuilder.toString();
        LOGGER.info(sql);
        this.getStatement().execute(sql);
    }

    private void setScriptOutputAddress() throws SQLException {
        final String hostIp = this.testInterface.getTestHostIpAddress();
        if (hostIp != null) {
            final String sql = "ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS='" + hostIp + ":" + SCRIPT_OUTPUT_PORT + "';";
            LOGGER.info(sql);
            this.getStatement().execute(sql);
        }
    }

    /**
     * Get if Virtual Schema debugging was enabled by the user.
     *
     * Therefore add to the jvm options {@code -Dtests.debug="virtualSchema"}
     *
     * @return {@code <true>} if set property is equal to "all"
     */
    private boolean isVirtualSchemaDebuggingEnabled() {
        final String noDebugProperty = System.getProperty("tests.debug");
        return noDebugProperty != null && (noDebugProperty.equals("all") || noDebugProperty.equals("virtualSchema"));
    }

    /**
     * Get if UDF debugging was enabled by the user.
     * 
     * Therefore add to the jvm options {@code -Dtests.debug="all"}
     *
     * @return {@code <true>} if set property is equal to "all"
     */
    private boolean isUdfDebuggingEnabled() {
        final String noDebugProperty = System.getProperty("tests.debug");
        return noDebugProperty != null && noDebugProperty.equals("all");
    }

    private boolean isProfilingEnabled() {
        final String profilingProperty = System.getProperty("tests.profiling");// enable profiling by setting
        // -Dtests.profiling="true"
        return profilingProperty != null && profilingProperty.equals("true");
    }

    /**
     * Runs {@code CREATE VIRTUAL SCHEMA} on Exasol test container.
     *
     * @param name               name for the newly created schema
     * @param dynamodbConnection name of the connection to use
     * @throws SQLException on SQL error
     */
    public void createDynamodbVirtualSchema(final String name, final String dynamodbConnection, final String mapping)
            throws SQLException {
        String createStatement = "CREATE VIRTUAL SCHEMA " + name + "\n" + "    USING " + ADAPTER_SCHEMA + "."
                + DYNAMODB_ADAPTER + " WITH\n" + "    CONNECTION_NAME = '" + dynamodbConnection + "'\n"
                + "   SQL_DIALECT     = 'DYNAMO_DB'\n" + "	  MAPPING = '" + mapping + "'";
        final String hostIp = this.testInterface.getTestHostIpAddress();
        if (hostIp != null) {
            createStatement += "\n   DEBUG_ADDRESS   = '" + hostIp + ":" + LOGGER_PORT + "'\n"
                    + "   LOG_LEVEL       =  'ALL'";
            if (isVirtualSchemaDebuggingEnabled()) {
                createStatement += "\n   MAX_PARALLEL_UDFS   = '1'\n";
            }
        }
        createStatement += ";";
        this.getStatement().execute(createStatement);
    }

    /**
     * Runs the SQL {@code DESCRIBE} command on a given table.
     *
     * @param schema schema name. Use quotes if lower case name
     * @param table  table name. Use quotes if lower case name
     * @return Map with column name as key and column type as value
     * @throws SQLException on SQL error
     */
    public Map<String, String> describeTable(final String schema, final String table) throws SQLException {
        final ResultSet describeResult = getStatement().executeQuery("DESCRIBE " + schema + "." + table + ";");
        final Map<String, String> columns = new HashMap<>();
        while (describeResult.next()) {
            columns.put(describeResult.getString(1), describeResult.getString(2));
        }
        return columns;
    }

    /**
     * Tests if a schema exists for the given name.
     *
     * @param schema name of the schema to look up
     * @return {@code <true> if the schema exists}
     */
    public boolean testIfSchemaExists(final String schema) {
        try {
            getStatement().executeQuery("OPEN SCHEMA " + schema + ";");
        } catch (final SQLException exception) {
            if (exception.getMessage().contains("not found")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Runs SQL {@code REFRESH} command for a given virtual schema.
     *
     * @param schemaName name of the virtual schema to refresh. Use quotes if lower case name.
     * @throws SQLException on SQL error
     */
    public void refreshVirtualSchema(final String schemaName) throws SQLException {
        this.getStatement().execute("ALTER VIRTUAL SCHEMA " + schemaName + " REFRESH;");
    }

    /**
     * Runs SQL {@code DROP VIRTUAL SCHEMA} command for a given virtual schema.
     *
     * @param schemaName name of the virtual schema to drop. Use quotes if lower case * name.
     * @throws SQLException on SQL error
     */
    public void dropVirtualSchema(final String schemaName) throws SQLException {
        this.getStatement().execute("DROP VIRTUAL SCHEMA IF EXISTS " + schemaName + " CASCADE;");
    }

    /**
     * Uploads the dynamodb adapter jar to the exasol test container.
     */
    public void uploadDynamodbAdapterJar() throws InterruptedException, BucketAccessException, TimeoutException {
        this.testInterface.uploadFileToBucketfs(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        this.testInterface.uploadFileToBucketfs(PATH_TO_JACOCO_JAR, JACOCO_JAR_NAME);
        if (isProfilingEnabled()) {
            final Path agentPath = Path.of("../", PROFILING_AGENT_FILE_NAME);
            if (!agentPath.toFile().exists()) {
                throw new IllegalStateException(
                        "Profiling was turned on using -Dtests.profiling=true but no profiling agent was provided. \n"
                                + "Please download the honest-profiler form https://github.com/jvm-profiling-tools/honest-profiler/ and place the "
                                + PROFILING_AGENT_FILE_NAME + " in thew directory above this project.");
            }
            this.testInterface.uploadFileToBucketfs(agentPath, PROFILING_AGENT_FILE_NAME);
        }
    }

    public void uploadMapping(final String name) throws InterruptedException, BucketAccessException, TimeoutException {
        uploadMapping(name, "mappings/" + name);
    }

    public void uploadMapping(final String name, final String destinationName)
            throws InterruptedException, BucketAccessException, TimeoutException {
        this.testInterface.uploadFileToBucketfs(Path.of("src", "test", "resources", name), destinationName);
    }

    public void cleanup() throws SQLException {
        cleanupObjects();
        cleanupConnections();
    }

    private void cleanupConnections() throws SQLException {
        final ResultSet resultSet = this.statement.executeQuery("SELECT CONNECTION_NAME FROM EXA_ALL_CONNECTIONS;");
        while (resultSet.next()) {
            final String connectionName = resultSet.getString("CONNECTION_NAME");
            final String dropCommand = "DROP CONNECTION IF EXISTS \"" + connectionName + "\"";
            LOGGER.info(dropCommand);
            this.statement.executeUpdate(dropCommand);
        }
    }

    private void cleanupObjects() throws SQLException {
        final ResultSet resultSet = this.statement.executeQuery(
                "SELECT OBJECT_NAME, OBJECT_TYPE, OBJECT_IS_VIRTUAL FROM SYS.EXA_ALL_OBJECTS WHERE OWNER = 'SYS' ORDER BY CREATED DESC;");
        while (resultSet.next()) {
            final String objectName = resultSet.getString("OBJECT_NAME");
            final String objectType = (resultSet.getBoolean("OBJECT_IS_VIRTUAL") ? "VIRTUAL " : "")
                    + resultSet.getString("OBJECT_TYPE");
            if (objectType.equals("VIRTUAL TABLE")) {
                continue;
            }
            final boolean addCascade = objectType.equals("SCHEMA") || objectType.equals("VIRTUAL SCHEMA");
            final String dropCommand = "DROP " + objectType + " IF EXISTS \"" + objectName + "\""
                    + (addCascade ? " CASCADE" : "");
            LOGGER.info(dropCommand);
            this.statement.executeUpdate(dropCommand);
        }
    }
}
