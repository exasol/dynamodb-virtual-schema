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
     * @param path: bucketfs path, e.g. {@code /bfsdefault/default/folder/file.txt}
     * @return File defined by the path
     * @throws IllegalArgumentException if the path is invalid or outside of the BucketFS.
     */
    public File openFile(final String path) {
        final String bucketfsPath = BUCKETFS_BASIC_PATH + path;
        final File selectedFile = new File(bucketfsPath);
        preventInjection(selectedFile);
        return selectedFile;
    }

    private void preventInjection(final File file) {
        try {
            final String absolute = file.getCanonicalPath();
            if (!absolute.startsWith(BUCKETFS_BASIC_PATH)) {
                throw new IllegalArgumentException(
                        "Given path (" + file.getCanonicalPath() + ") is outside of bucketfs.");

            }
        } catch (final IOException exception) {
            throw new IllegalArgumentException("Error in file path: " + file.getAbsolutePath());
        }
    }
}
