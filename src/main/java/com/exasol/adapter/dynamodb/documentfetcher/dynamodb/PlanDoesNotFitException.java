package com.exasol.adapter.dynamodb.documentfetcher.dynamodb;

/**
 * This exception is thrown by a QueryPlanFactory if the plan does not fit for the given query.
 */
public class PlanDoesNotFitException extends RuntimeException {
    private static final long serialVersionUID = -3257463856415639740L;

    PlanDoesNotFitException(final String message) {
        super(message);
    }
}
