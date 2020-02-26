package util;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;


public class ExasolTestUtils {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "dynamodb-virtual-schemas-adapter-dist-0.0.1.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    public static final String ADAPTER_SCHEMA = "ADAPTER";
    public static final String DYNAMODB_ADAPTER = "DYNAMODB_ADAPTER";

    private ExasolContainer<? extends ExasolContainer<?>> container;
    private Statement statement;

    public ExasolTestUtils(ExasolContainer<? extends ExasolContainer<?>> container) throws SQLException {
        final Connection connection = container.createConnectionForUser(container.getUsername(),
                container.getPassword());
        this.statement = connection.createStatement();
        this.container = container;
    }

    public Statement getStatement(){
        return this.statement;
    }

    public void uploadDynamodbAdapterJar() throws InterruptedException, BucketAccessException, TimeoutException {
        final Bucket bucket = container.getDefaultBucket();
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    }

    public void createTestSchema(final String schemaName) throws SQLException {
        statement.execute("CREATE SCHEMA " + schemaName);
    }

    public void createConnection(String name, String to, String user, String pass) throws SQLException {
        statement.execute("CREATE CONNECTION " + name +  " TO '" + to + "' USER '" + user + "' IDENTIFIED BY '" + pass + "';");
    }

    public void createAdapterScript() throws SQLException {
        this.createTestSchema(ADAPTER_SCHEMA);
        statement.execute("CREATE OR REPLACE JAVA ADAPTER SCRIPT " + ADAPTER_SCHEMA + "." + DYNAMODB_ADAPTER + " AS\n" +
                "    %scriptclass com.exasol.adapter.RequestDispatcher;\n" +
                "    %jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION +  ";\n" +
                "/");
    }

    public void createDynamodbVirtualSchema(String name, String dynamodbConnection) throws SQLException {
        statement.execute("CREATE VIRTUAL SCHEMA " + name + "\n" +
                "    USING " + ADAPTER_SCHEMA + "." + DYNAMODB_ADAPTER + " WITH\n" +
                "    CONNECTION_NAME = '" + dynamodbConnection+ "'\n" +
                "   SQL_DIALECT     = 'Dynamodb'\n" +
                "   DEBUG_ADDRESS   = '10.0.2.15:3000'\n" +//TODO find host ip dynamiclaly
                "   LOG_LEVEL       =  'ALL';");
    }

    public void sleepForDebugging(){
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
