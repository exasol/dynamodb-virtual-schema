package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * Tests for {@link GenericSchemaMappingVisitor}
 */
public class GenericSchemaMappingVisitorTest {
    @Test
    void testUnimplementedException(){
        final Method[] methods = GenericSchemaMappingVisitor.class.getDeclaredMethods();
        for (final Method method : methods) {
            final Moc moc = new Moc();
            if(!method.getName().equals("visit"))
                continue;
            final InvocationTargetException exception = assertThrows(InvocationTargetException.class, () -> method.invoke(moc, new Object[]{ null }));
            assertThat(exception.getCause().getClass(), equalTo(UnsupportedOperationException.class));
        }
    }

    private static class Moc extends GenericSchemaMappingVisitor{

        @Override
        public Void visit(final SqlStatementSelect select) throws AdapterException {
            return null;
        }
    }
}
