package com.exasol.bucketfs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;

import org.junit.jupiter.api.Test;

public class BucketfsFileFactoryTest {

	@Test
	public void testOpenFile() throws BucketfsPathException {
		final String path = "/bfsdefault/default/folder/file.txt";
		final File file = new BucketfsFileFactory().openFile(path);
		assertThat(file.getAbsolutePath(), equalTo("/buckets/bfsdefault/default/folder/file.txt"));
	}

	@Test
	public void testInjection() {
		final String injectionPath = "/../etc/secrets.conf";
		final BucketfsPathException exception = assertThrows(BucketfsPathException.class,
				() -> new BucketfsFileFactory().openFile(injectionPath));
		assertThat(exception.getCausingPath(), equalTo("/etc/secrets.conf"));
	}

}
