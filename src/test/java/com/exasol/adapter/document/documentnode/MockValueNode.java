package com.exasol.adapter.document.documentnode;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "MockValueNode{" + "value='" + this.value + '\'' + '}';
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final MockValueNode that = (MockValueNode) other;

        return Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return this.value != null ? this.value.hashCode() : 0;
    }
}
