package com.exasol.utils;

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
     * @param serializable Object to be serialized
     * @return base64 string
     * @throws IOException if serialization fails
     */
    public static String serializeToString(final Serializable serializable) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
        objectOutputStream.writeObject(serializable);
        objectOutputStream.flush();
        return Base64.getEncoder().encodeToString(byteOutputStream.toByteArray());
    }

    /**
     * Deserializes an object from a base64 string.
     * 
     * @param serialized base64 string created by {@link #serializeToString(Serializable)}
     * @return deserialized object. Cast this object to your class.
     * @throws IOException            if deserialization fails
     * @throws ClassNotFoundException if deserialization fails
     */
    public static Object deserializeFromString(final String serialized) throws IOException, ClassNotFoundException {
        final byte[] data = Base64.getDecoder().decode(serialized);
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return objectInputStream.readObject();
    }
}
