package com.exasol.adapter.dynamodb.cdata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    private static final String JDBC_ADAPTER_JAR_NAME = "cdata.jdbc.amazondynamodb.jar";
    private static final Path PATH_TO_JDBC_ADAPTER_JAR = Path.of("../", JDBC_ADAPTER_JAR_NAME);
    private static final String CONNECTION_NAME = "MY_CONNECTION";
    private static AwsExasolTestInterface exasolTestInterface;

    @BeforeAll
    static void beforeAll()
            throws SQLException, BucketAccessException, InterruptedException, java.util.concurrent.TimeoutException,
            IOException, NoSuchAlgorithmException, KeyManagementException, XmlRpcException {
        final CdataCredentialProvider cdataCredentialProvider = new CdataCredentialProvider();

        exasolTestInterface = new AwsExasolTestInterface();
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
        exasolTestInterface.createConnection(CONNECTION_NAME,
                "jdbc:amazondynamodb:Access Key=" + cdataCredentialProvider.getAwsAccessKey() + ";Secret Key="
                        + cdataCredentialProvider.getAwsSecretKey()
                        + ";Domain=amazonaws.com;Region=frankfurt;Verbosity=3;Cache Location=/tmp/;RTK="
                        + cdataCredentialProvider.getRtk() + ";Other = DefaultColumnSize = 200000;",
                "not used", "anyway");
    }



    @Test
    void testCountAllWithIncreasedLimitsAndProjection() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\" FROM \"open_library_test\"')");
        resultSet.next();
    }

    @Test
    void testCountAllWithIncreasedLimitsWith3Columns() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8, c3 VARCHAR(200000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\", \"title\" FROM \"open_library_test\"')");
        resultSet.next();
    }

    @Test
    void testCountAllWithIncreasedLimitsWith4Columns() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8, c2 VARCHAR(255) UTF8, c3 VARCHAR(200000) UTF8, c4 VARCHAR(2000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\", \"revision\", \"title\", \"title_prefix\" FROM \"open_library_test\"')");
        resultSet.next();
    }

    @Test
    void testToJson() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(200000) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"publishers\" FROM \"open_library_test\"')");
    }

    @Test
    void testCountAuthorsTable() throws SQLException {
        final ResultSet resultSet = exasolTestInterface.getStatement().executeQuery(
                "SELECT * FROM (IMPORT INTO (c1 VARCHAR(255) UTF8) FROM JDBC AT MY_CONNECTION STATEMENT 'SELECT \"key\" FROM \"open_library_test.authors\";');");
        resultSet.next();
        // final int count = resultSet.getInt(1);
        // assertThat(count, equalTo(858180));
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
