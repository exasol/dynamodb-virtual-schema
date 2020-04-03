package com.exasol.bucketfs;

/**
 * Exception that is thrown if path is invalid or on injection.
 */
public class BucketfsPathException extends Exception {
	private final String causingPath;

	/**
	 * Constructor for Exception
	 * 
	 * @param message
	 *            exception message
	 * @param causingPath
	 *            path that caused the exception
	 */
	public BucketfsPathException(final String message, final String causingPath) {
		super(message);
		this.causingPath = causingPath;
	}

	/**
	 * Getter for the invalid path name.
	 * 
	 * @return String containing absolute path.
	 */
	public String getCausingPath() {
		return this.causingPath;
	}
}
