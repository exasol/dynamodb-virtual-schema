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
public abstract class DynamodbResultWalker implements Serializable {
	private static final long serialVersionUID = 7478280663406355912L;
	private final DynamodbResultWalker next;

	/**
	 * Constructor as last part of the {@link DynamodbResultWalker} chain.
	 */
	DynamodbResultWalker() {
		this.next = null;
	}

	/**
	 * Constructor as non last part of the {@link DynamodbResultWalker} chain.
	 */
	DynamodbResultWalker(final DynamodbResultWalker next) {
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
		return this.walk(attributeValueRepresentingItem);
	}

	private AttributeValue walk(final AttributeValue attributeValue) throws DynamodbResultWalkerException {
		return applyNext(applyThis(attributeValue));
	}

	abstract AttributeValue applyThis(AttributeValue attributeValue) throws DynamodbResultWalkerException;

	private AttributeValue applyNext(final AttributeValue attributeValue) throws DynamodbResultWalkerException {
		if (this.next != null) {
			return this.next.walk(attributeValue);
		}
		return attributeValue;
	}

	/**
	 * Enum defining the behavior of {@link #walk(AttributeValue)} if the modeled
	 * path does not exist:
	 */
	public enum LookupFailBehaviour {
		/**
		 * an {@link DynamodbResultWalkerException} is thrown
		 */
		EXCEPTION,
		/**
		 * {@code null} is returned
		 */
		NULL
	}

	/**
	 * Exception fired when the modeled path does not exist.
	 */
	@SuppressWarnings("serial")
	public static class DynamodbResultWalkerException extends AdapterException {
		DynamodbResultWalkerException(final String message) {
			super(message);
		}
	}
}
