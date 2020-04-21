package com.exasol.adapter.dynamodb.queryrunner;

public class PlanDoesNotFitException extends Exception {
    private static final long serialVersionUID = -3257463856415639740L;

    PlanDoesNotFitException(final String message) {
        super(message);
    }
}
