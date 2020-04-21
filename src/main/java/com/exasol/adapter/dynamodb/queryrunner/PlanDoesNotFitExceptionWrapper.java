package com.exasol.adapter.dynamodb.queryrunner;

class PlanDoesNotFitExceptionWrapper extends RuntimeException {
    private final PlanDoesNotFitException wrappedException;

    PlanDoesNotFitExceptionWrapper(final PlanDoesNotFitException wrappedException) {
        this.wrappedException = wrappedException;
    }

    PlanDoesNotFitException getWrappedException() {
        return this.wrappedException;
    }
}
