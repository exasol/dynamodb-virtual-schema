package com.exasol.adapter.document;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.bucketfs.BucketAccessException;

/**
 * Exasol test interface using an Exasol cluster running on AWS, created via the terraform file in cloudSetup/.
 */
public abstract class AbstractExasolTestInterface implements ExasolTestInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExasolTestInterface.class);
    private static final String BUCKET_NAME = "default";
    private final HttpClient client = HttpClient.newBuilder().build();

    @Override
    public abstract void teardown();

    @Override
    public abstract String getTestHostIpAddress();

    protected abstract String getContainerUrl();

    protected abstract int getBucketFsPort();

    protected abstract String getBucketFsReadPassword();

    protected abstract String getBucketFsWritePassword();

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
            return new URI("http", null, getContainerUrl(), getBucketFsPort(), "/" + BUCKET_NAME + "/" + pathInBucket,
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
                .encodeToString(
                        (write ? ("w:" + getBucketFsWritePassword()) : ("r:" + getBucketFsReadPassword())).getBytes());
    }

    @Override
    public abstract Connection getConnection() throws SQLException, IOException;
}
