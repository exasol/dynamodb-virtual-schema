package com.exasol.bucketfs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;

import org.junit.jupiter.api.Test;

class BucketfsFileFactoryTest {

    @Test
    void testOpenFile() {
        final String path = "/bfsdefault/default/folder/file.txt";
        final File file = new BucketfsFileFactory().openFile(path);
        assertThat(file.getAbsolutePath(), equalTo("/buckets/bfsdefault/default/folder/file.txt"));
    }

    @Test
    void testInjection() {
        final String injectionPath = "/../etc/secrets.conf";
        final BucketfsFileFactory bucketfsFileFactory = new BucketfsFileFactory();
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> bucketfsFileFactory.openFile(injectionPath));
        assertThat(exception.getMessage(), equalTo("Given path (/etc/secrets.conf) is outside of bucketfs."));
    }

}
