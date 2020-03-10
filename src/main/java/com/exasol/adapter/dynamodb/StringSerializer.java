package com.exasol.adapter.dynamodb;

import java.io.*;
import java.util.Base64;

/**
 * Helper class for serialization to a base64 strings.
 */
public class StringSerializer {
	private StringSerializer() {
	}

	/**
	 * Serializes a given object to a base64 string.
	 * 
	 * @param serializable
	 *            Object to be serialized
	 * @return base64 string
	 * @throws IOException
	 */
	public static String serializeToString(final Serializable serializable) throws IOException {
		final ByteArrayOutputStream bo = new ByteArrayOutputStream();
		final ObjectOutputStream so = new ObjectOutputStream(bo);
		so.writeObject(serializable);
		so.flush();
		return Base64.getEncoder().encodeToString(bo.toByteArray());
	}

	/**
	 * Deserializes an object from base64 string.
	 * 
	 * @param serialized
	 *            base64 string created by {@link #serializeToString(Serializable)}
	 * @return deserialized object. Cast this object to your class.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object deserializeFromString(final String serialized) throws IOException, ClassNotFoundException {
		final byte[] data = Base64.getDecoder().decode(serialized);
		final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
		final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		return objectInputStream.readObject();
	}
}
