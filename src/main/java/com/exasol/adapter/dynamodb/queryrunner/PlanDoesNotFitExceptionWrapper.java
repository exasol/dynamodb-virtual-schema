package com.exasol.adapter.dynamodb.queryrunner;

/**
 * This exception wraps the checked {@link PlanDoesNotFitException} into a {@link RuntimeException} in order to tunnel
 * it out of visitors.
 */
class PlanDoesNotFitExceptionWrapper extends RuntimeException {
    private static final long serialVersionUID = 9046783561100888641L;
    private final PlanDoesNotFitException wrappedException;

    PlanDoesNotFitExceptionWrapper(final PlanDoesNotFitException wrappedException) {
        this.wrappedException = wrappedException;
    }

    PlanDoesNotFitException getWrappedException() {
        return this.wrappedException;
    }
}
