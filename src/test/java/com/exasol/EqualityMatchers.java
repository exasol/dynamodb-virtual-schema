package com.exasol;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

public class EqualityMatchers {

    public static void assertSymmetricEqualWithHashAndEquals(final Object a, final Object b) {
        assertAll(//
                () -> assertThat(a.equals(b), equalTo(true)), //
                () -> assertThat(b.equals(a), equalTo(true)), //
                () -> assertThat(a.hashCode(), equalTo(b.hashCode())), //
                () -> assertThat(b.hashCode(), equalTo(a.hashCode()))//
        );
    }

    public static void assertSymmetricNotEqualWithHashAndEquals(final Object a, final Object b) {
        assertAll(//
                () -> assertThat(a.equals(b), equalTo(false)), //
                () -> assertThat(b.equals(a), equalTo(false)), //
                () -> assertThat(a.hashCode(), not(equalTo(b.hashCode()))), //
                () -> assertThat(b.hashCode(), not(equalTo(a.hashCode())))//
        );
    }
}
