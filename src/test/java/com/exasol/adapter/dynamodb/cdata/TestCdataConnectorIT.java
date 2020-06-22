package com.exasol.adapter.dynamodb.cdata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.apache.xmlrpc.XmlRpcException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.dynamodb.AwsExasolTestInterface;
import com.exasol.bucketfs.BucketAccessException;

/*
 * This is an performance test for a Virtual Schema using the generic JDBC driver and the CData connector.
 * 
 * Preparations:
 * - Get CData JDBC connector jar
 * - place jar one folder above this repo
 * - upload driver jar to EXAOperation
 * - disable security manager for driver
 * - ask CDATA support for RTK key
 * - create ~/cdata_credentials.json
 * - define aws_access_key, aws_secret_key, rtk in the JSON file
 */
@Tag("integration")
public class TestCdataConnectorIT {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtualschema-jdbc-adapter-dist-4.0.1.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("../", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    private static final String JDBC_ADAPTER_JAR_NAME = "cdata.jdbc.amazondynamodb.jar";
    private static final Path PATH_TO_JDBC_ADAPTER_JAR = Path.of("../", JDBC_ADAPTER_JAR_NAME);
    private static final String TEST_SCHEMA = "TEST";
    private static final String ADAPTER_SCHEMA = "ADAPTER";
    private static final String CONNECTION_NAME = "MY_CONNECTION";
    private static AwsExasolTestInterface exasolTestInterface;

    @BeforeAll
    static void beforeAll()
            throws SQLException, BucketAccessException, InterruptedException, java.util.concurrent.TimeoutException,
            IOException, NoSuchAlgorithmException, KeyManagementException, XmlRpcException {
        final CdataCredentialProvider cdataCredentialProvider = new CdataCredentialProvider();

        exasolTestInterface = new AwsExasolTestInterface();
        exasolTestInterface.dropVirtualSchema(TEST_SCHEMA);
        exasolTestInterface.dropSchema(ADAPTER_SCHEMA);
        exasolTestInterface.dropConnection(CONNECTION_NAME);
        try {
            exasolTestInterface.getExaOperationInterface().createAndUploadJdbcDriver("cdata",
                    "cdata.jdbc.amazondynamodb.AmazonDynamoDBDriver", "jdbc:amazondynamodb:", true,
                    PATH_TO_JDBC_ADAPTER_JAR.toFile());
        } catch (final XmlRpcException exception) {
            if (!exception.getMessage().equals("JDBC driver name is already used for another JDBC driver.")) {
                throw exception;
            }
        }
        uploadExternalJars();
        Thread.sleep(1000 * 5);
        exasolTestInterface.createConnection(CONNECTION_NAME,
                "jdbc:amazondynamodb:Access Key=" + cdataCredentialProvider.getAwsAccessKey() + ";Secret Key="
                        + cdataCredentialProvider.getAwsSecretKey()
                        + ";Domain=amazonaws.com;Region=frankfurt;Verbosity=3;Cache Location=/tmp/;RTK="
                        + cdataCredentialProvider.getRtk() + ";Other = DefaultColumnSize = 200000;",
                "not used", "anyway");
        createAdapterScript();
        createVirtualSchema(TEST_SCHEMA);
    }

    public static void createAdapterScript() throws SQLException {
        exasolTestInterface.createSchema(ADAPTER_SCHEMA);
        final String sql = "CREATE JAVA ADAPTER SCRIPT " + ADAPTER_SCHEMA + ".JDBC_ADAPTER_SCRIPT AS\n"
                + "  %scriptclass com.exasol.adapter.RequestDispatcher;\n" + "  %jar /buckets/bfsdefault/default/"
                + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n" + "  %jar /buckets/bfsdefault/default/"
                + JDBC_ADAPTER_JAR_NAME + ";\n" + "/";
        exasolTestInterface.getStatement().execute(sql);
    }

    /**
     * Uploads the cdata jar to the exasol test container.
     */
    private static void uploadExternalJars() throws InterruptedException, BucketAccessException, TimeoutException {
        exasolTestInterface.uploadFileToBucketfs(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        exasolTestInterface.uploadFileToBucketfs(PATH_TO_JDBC_ADAPTER_JAR, JDBC_ADAPTER_JAR_NAME);
    }

    public static void createVirtualSchema(final String name) throws SQLException {
        String createStatement = "CREATE VIRTUAL SCHEMA " + name + "\n" + "    USING " + ADAPTER_SCHEMA
                + ".JDBC_ADAPTER_SCRIPT WITH\n" + "    CONNECTION_NAME = '" + CONNECTION_NAME + "'\n"
                + "   SQL_DIALECT     = 'GENERIC'\n" + "";
        createStatement += ";";
        exasolTestInterface.getStatement().execute(createStatement);
    }

    @Test
    void testCountAll() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement()
                .executeQuery("SELECT COUNT(*) FROM TEST.\"open_library_test\"");
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(148163));
    }

    @Test
    void testCountAllWithIncreasedLimits() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT COUNT(*) FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8, c3 VARCHAR(200000) UTF8, c4 VARCHAR(200000) UTF8, c5 VARCHAR(200000) UTF8, c6 VARCHAR(200000) UTF8, c7 VARCHAR(200000) UTF8, c8 VARCHAR(200000) UTF8, c9 VARCHAR(200000) UTF8, c10 VARCHAR(200000) UTF8, c11 VARCHAR(200000) UTF8, c12 VARCHAR(200000) UTF8, c13 VARCHAR(200000) UTF8, c14 VARCHAR(200000) UTF8, c15 VARCHAR(200000) UTF8, c16 VARCHAR(200000) UTF8, c17 VARCHAR(200000) UTF8, c18 VARCHAR(200000) UTF8, c19 VARCHAR(200000) UTF8, c20 VARCHAR(200000) UTF8, c21 VARCHAR(200000) UTF8, c22 VARCHAR(200000) UTF8, c23 VARCHAR(200000) UTF8, c24 VARCHAR(200000) UTF8, c25 VARCHAR(200000) UTF8, c26 VARCHAR(200000) UTF8, c27 VARCHAR(200000) UTF8, c28 VARCHAR(200000) UTF8, c29 VARCHAR(200000) UTF8, c30 VARCHAR(200000) UTF8, c31 VARCHAR(200000) UTF8, c32 VARCHAR(200000) UTF8, c33 VARCHAR(200000) UTF8, c34 VARCHAR(200000) UTF8, c35 VARCHAR(200000) UTF8, c36 VARCHAR(200000) UTF8, c37 VARCHAR(200000) UTF8, c38 VARCHAR(200000) UTF8, c39 VARCHAR(200000) UTF8, c40 VARCHAR(200000) UTF8, c41 VARCHAR(200000) UTF8, c42 VARCHAR(200000) UTF8, c43 VARCHAR(200000) UTF8, c44 VARCHAR(200000) UTF8, c45 VARCHAR(200000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT  FROM \"open_library_test\"')");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(148163));
    }

    @Test
    void testCountAllWithIncreasedLimitsAndProjection() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT COUNT(*) FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\" FROM \"open_library_test\"')");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(148163));
    }

    @Test
    void testSelectSingleRow() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT COUNT(*) FROM TEST.\"open_library_test\" WHERE \"key\" = '/authors/OL7124039A' AND \"revision\" = 1");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(1));
    }
}
