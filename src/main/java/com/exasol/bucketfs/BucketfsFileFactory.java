package com.exasol.bucketfs;

import java.io.File;
import java.io.IOException;

/**
 * Factory for files in bucketfs. Breaking out of the bucketfs using injection is prevented.
 */
public class BucketfsFileFactory {

    @SuppressWarnings("java:S1075") // this is not a configurable path
    private static final String BUCKETFS_BASIC_PATH = "/buckets";

    /**
     * Opens a file from bucketfs by a given path.
     * 
     * @param path: bucketfs path. e.g. {@code /bfsdefault/default/folder/file.txt}
     * @return File defined by the path
     * @throws BucketfsPathException if path is invalid or on injection
     */
    public File openFile(final String path) throws BucketfsPathException {
        final String bucketfsPath = BUCKETFS_BASIC_PATH + path;
        final File selectedFile = new File(bucketfsPath);
        preventInjection(selectedFile);
        return selectedFile;
    }

    private void preventInjection(final File file) throws BucketfsPathException {
        try {
            final String absolute;
            absolute = file.getCanonicalPath();
            if (!absolute.startsWith(BUCKETFS_BASIC_PATH)) {
                throw new BucketfsPathException("Given path is outside of bucketfs", file.getCanonicalPath());
            }
        } catch (final IOException e) {
            throw new BucketfsPathException("Error in file path: " + file.getAbsolutePath(), file.getAbsolutePath());
        }
    }
}
