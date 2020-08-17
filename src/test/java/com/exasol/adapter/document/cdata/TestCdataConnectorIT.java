package com.exasol.adapter.document.cdata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.xmlrpc.XmlRpcException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.document.AwsExasolTestInterface;
import com.exasol.adapter.document.ExasolTestDatabaseBuilder;
import com.exasol.adapter.document.IntegrationTestSetup;
import com.exasol.bucketfs.BucketAccessException;

/*
 * This is an performance test for a Virtual Schema using the generic JDBC driver and the CData connector.
 * 
 * Preparations:
 * - Get CData JDBC connector jar
 * - place jar one folder above this repo
 * - ask CDATA support for RTK key
 * - create ~/cdata_credentials.json
 * - define aws_access_key, aws_secret_key, rtk in the JSON file
 */
@Tag("integration")
@SuppressWarnings("java:S2699") // tests in this class do not contains assertions because they are performance tests
class TestCdataConnectorIT {
    private static final String JDBC_ADAPTER_JAR_NAME = "cdata.jdbc.amazondynamodb.jar";
    private static final Path PATH_TO_JDBC_ADAPTER_JAR = Path.of("../", JDBC_ADAPTER_JAR_NAME);
    private static final String CONNECTION_NAME = "MY_CONNECTION";
    private static AwsExasolTestInterface exasolTestInterface;
    private static ExasolTestDatabaseBuilder exasolTestSetup;

    @BeforeAll
    static void beforeAll()
            throws SQLException, BucketAccessException, InterruptedException, java.util.concurrent.TimeoutException,
            IOException, NoSuchAlgorithmException, KeyManagementException, XmlRpcException {
        final CdataCredentialProvider cdataCredentialProvider = new CdataCredentialProvider();
        final IntegrationTestSetup integrationTestSetup = new IntegrationTestSetup();
        exasolTestInterface = (AwsExasolTestInterface) integrationTestSetup.getExasolTestInterface();
        exasolTestSetup = new ExasolTestDatabaseBuilder(exasolTestInterface);
        exasolTestSetup.dropConnection(CONNECTION_NAME);
        try {
            exasolTestInterface.getExaOperationInterface().createAndUploadJdbcDriver("cdata",
                    "cdata.jdbc.amazondynamodb.AmazonDynamoDBDriver", "jdbc:amazondynamodb:", true,
                    PATH_TO_JDBC_ADAPTER_JAR.toFile());
        } catch (final XmlRpcException exception) {
            if (!exception.getMessage().equals("JDBC driver name is already used for another JDBC driver.")) {
                throw exception;
            }
        }
        exasolTestSetup.createConnection(CONNECTION_NAME,
                "jdbc:amazondynamodb:Access Key=" + cdataCredentialProvider.getAwsAccessKey() + ";Secret Key="
                        + cdataCredentialProvider.getAwsSecretKey()
                        + ";Domain=amazonaws.com;Region=frankfurt;Verbosity=3;Cache Location=/tmp/;RTK="
                        + cdataCredentialProvider.getRtk() + ";Other = DefaultColumnSize = 200000;",
                "not used", "anyway");
    }

    @AfterAll
    static void afterAll() {
        exasolTestInterface.teardown();
    }

    @Test
    void testCountAllWithIncreasedLimitsAndProjection() throws SQLException {
        final ResultSet resultSet = exasolTestSetup.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\" FROM \"open_library_test\"')");
        resultSet.next();
    }

    @Test
    void testCountAllWithIncreasedLimitsWith3Columns() throws SQLException {
        final ResultSet resultSet = exasolTestSetup.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8, c3 VARCHAR(200000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\", \"title\" FROM \"open_library_test\"')");
        resultSet.next();
    }

    @Test
    void testCountAllWithIncreasedLimitsWith4Columns() throws SQLException {
        final ResultSet resultSet = exasolTestSetup.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8, c3 VARCHAR(200000) UTF8, c4 VARCHAR(2000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\", \"title\", \"title_prefix\" FROM \"open_library_test\"')");
        resultSet.next();
    }

    @Test
    void testToJson() throws SQLException {
        final ResultSet resultSet = exasolTestSetup.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(200000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"publishers\" FROM \"open_library_test\"')");
    }

    @Test
    void testCountAuthorsTable() throws SQLException {
        final ResultSet resultSet = exasolTestSetup.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\" FROM \"open_library_test.authors\";');");
        resultSet.next();
        // final int count = resultSet.getInt(1);
        // assertThat(count, equalTo(858180));
    }

    @Test
    void testSelectSingleRow() throws SQLException {
        final ResultSet resultSet = exasolTestSetup.getStatement().executeQuery(
                "SELECT COUNT(*) FROM TEST.\"open_library_test\" WHERE \"key\" = '/authors/OL7124039A' AND \"revision\" = 1");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertThat(count, equalTo(1));
    }
}
