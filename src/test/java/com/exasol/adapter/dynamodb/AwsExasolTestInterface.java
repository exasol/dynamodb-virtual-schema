package com.exasol.adapter.dynamodb;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.TerraformInterface;
import com.exasol.bucketfs.BucketAccessException;

/**
 * Exasol test interface for exasol clusters running on AWS. Start the cluster by running {@code terraform apply} in
 * {@code /cloudSetup}
 */
public class AwsExasolTestInterface extends AbstractExasolTestInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsExasolTestInterface.class);
    private static final String SYS_USER_PW = "eXaSol1337DB";
    private static final int BUCKETFS_PORT = 2580;
    private static final String READ_PASSWORD = "readpw";
    private static final String WRITE_PASSWORD = "writepw";
    private static final String BUCKET_NAME = "default";
    private final HttpClient client = HttpClient.newBuilder().build();
    private final String exasolIp;

    public AwsExasolTestInterface() throws SQLException, IOException {
        this(new TerraformInterface().getExasolDataNodeIp());
    }

    private AwsExasolTestInterface(final String dataNodeIp) throws IOException, SQLException {
        super(getConnection(dataNodeIp));
        this.exasolIp = dataNodeIp;
    }

    public static Connection getConnection(final String exasolIpAddress) throws SQLException, IOException {
        return DriverManager.getConnection("jdbc:exa:" + exasolIpAddress + ":8563;schema=SYS", "sys", SYS_USER_PW);
    }

    @Override
    protected String getTestHostIpAddress() {
        return null;
    }

    @Override
    public void uploadFileToBucketfs(final Path localPath, final String bucketPath)
            throws InterruptedException, BucketAccessException, TimeoutException {
        final String extendedPathInBucket = extendPathInBucketDownToFilename(localPath, bucketPath);
        uploadFileNonBlocking(localPath, extendedPathInBucket);
    }

    private String extendPathInBucketDownToFilename(final Path localPath, final String pathInBucket) {
        return pathInBucket.endsWith("/") ? pathInBucket + localPath.getFileName() : pathInBucket;
    }

    private void uploadFileNonBlocking(final Path localPath, final String pathInBucket)
            throws InterruptedException, BucketAccessException {
        final URI uri = createWriteUri(pathInBucket);
        LOGGER.debug("Uploading file \"{}\" to bucket\": \"{}\"", localPath, uri);
        try {
            final int statusCode = httpPut(uri, HttpRequest.BodyPublishers.ofFile(localPath));
            if (statusCode != HttpURLConnection.HTTP_OK) {
                LOGGER.error("{}: Failed to upload file \"{}\" to \"{}\"", statusCode, localPath, uri);
                throw new BucketAccessException("Unable to upload file \"" + localPath + "\"" + " to ", statusCode,
                        uri);
            }
        } catch (final IOException exception) {
            throw new BucketAccessException("Unable to upload file \"" + localPath + "\"" + " to ", uri, exception);
        }
        LOGGER.debug("Successfully uploaded to \"{}\"", uri);
    }

    private URI createWriteUri(final String pathInBucket) throws BucketAccessException {
        try {
            return new URI("http", null, this.exasolIp, BUCKETFS_PORT, "/" + BUCKET_NAME + "/" + pathInBucket,
                    null, null).normalize();
        } catch (final URISyntaxException exception) {
            throw new BucketAccessException("Unable to create write URI.", exception);
        }
    }

    private int httpPut(final URI uri, final HttpRequest.BodyPublisher bodyPublisher)
            throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder(uri) //
                .PUT(bodyPublisher) //
                .header("Authorization", encodeBasicAuth(true)) //
                .build();
        final HttpResponse<String> response = this.client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }

    private String encodeBasicAuth(final boolean write) {
        return "Basic " + Base64.getEncoder() //
                .encodeToString((write ? ("w:" + WRITE_PASSWORD) : ("r:" + READ_PASSWORD)).getBytes());
    }
}
