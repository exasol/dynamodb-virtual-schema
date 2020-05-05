package com.exasol.adapter.dynamodb.documentnode;

public class MockValueNode implements DocumentValue<Object> {
    private static final long serialVersionUID = 3972505400925387082L;
    private final String value;

    public MockValueNode(final String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    @Override
    public void accept(final Object visitor) {

    }
}
