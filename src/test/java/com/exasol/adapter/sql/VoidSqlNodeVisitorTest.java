package com.exasol.adapter.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VoidSqlNodeVisitor}
 */
public class VoidSqlNodeVisitorTest {
    @Test
    void testUnimplementedException() {
        final Method[] methods = VoidSqlNodeVisitor.class.getDeclaredMethods();
        for (final Method method : methods) {
            final Moc moc = new Moc();
            if (!method.getName().equals("visit"))
                continue;
            final InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                    () -> method.invoke(moc, new Object[] { null }));
            assertThat(exception.getCause().getClass(), equalTo(UnsupportedOperationException.class));
        }
    }

    private static class Moc extends VoidSqlNodeVisitor {

        @Override
        public Void visit(final SqlStatementSelect select) {
            return null;
        }
    }
}
