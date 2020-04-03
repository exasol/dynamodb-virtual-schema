package com.exasol.dynamodb.resultwalker;

import com.exasol.adapter.AdapterException;

/**
 * Exception thrown when the modeled path does not exist.
 */
@SuppressWarnings("serial")
public class DynamodbResultWalkerException extends AdapterException {
	private final String currentPath;

	/**
	 * Creates an instance of {@link DynamodbResultWalkerException}.
	 *
	 * @param message
	 *            Exception message.
	 * @param currentPath
	 *            path to the result walker step that caused the exception
	 */
	DynamodbResultWalkerException(final String message, final String currentPath) {
		super(message);
		this.currentPath = currentPath;
	}

	/**
	 * Gives the path to the result walker step that caused the exception.
	 *
	 * @return String describing the path.
	 */
	public String getCurrentPath() {
		return this.currentPath;
	}
}
