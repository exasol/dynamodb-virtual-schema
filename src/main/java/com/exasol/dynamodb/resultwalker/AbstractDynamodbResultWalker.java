package com.exasol.dynamodb.resultwalker;

import java.io.Serializable;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.exasol.adapter.AdapterException;

/**
 * This class is used for modelling path expressions. For example the path
 * {@code author.name} could be model by:
 * {@code ObjectDynamodbResultWalker("author", ObjectDynamodbResultWalker("name"))}
 */
public abstract class AbstractDynamodbResultWalker implements Serializable {
	private static final long serialVersionUID = 7478280663406355912L;
	private final AbstractDynamodbResultWalker next;

	/**
	 * Constructor as last part of the {@link AbstractDynamodbResultWalker} chain.
	 */
	public AbstractDynamodbResultWalker() {
		this.next = null;
	}

	/**
	 * Constructor as non last part of the {@link AbstractDynamodbResultWalker}
	 * chain.
	 */
	public AbstractDynamodbResultWalker(final AbstractDynamodbResultWalker next) {
		this.next = next;
	}

	/**
	 * Entry point for the path resolution.
	 *
	 * @param item
	 * @return AttributeValue specified by the path modeled by a chain of this
	 *         objects
	 */
	public AttributeValue walk(final Map<String, AttributeValue> item) throws DynamodbResultWalkerException {
		final AttributeValue attributeValueRepresentingItem = new AttributeValue();
		attributeValueRepresentingItem.setM(item);
		return this.walk(attributeValueRepresentingItem, "");
	}

	private AttributeValue walk(final AttributeValue attributeValue, final String path)
			throws DynamodbResultWalkerException {
		return applyNext(applyThis(attributeValue, path), path);
	}

	protected abstract AttributeValue applyThis(AttributeValue attributeValue, String path)
			throws DynamodbResultWalkerException;
	protected abstract String stepDescription();

	private AttributeValue applyNext(final AttributeValue attributeValue, final String path)
			throws DynamodbResultWalkerException {
		if (this.next != null) {
			return this.next.walk(attributeValue, path + stepDescription());
		}
		return attributeValue;
	}

	/**
	 * Exception fired when the modeled path does not exist.
	 */
	@SuppressWarnings("serial")
	public static class DynamodbResultWalkerException extends AdapterException {
		private final String currentPath;

		/**
		 * Constructor.
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
}
