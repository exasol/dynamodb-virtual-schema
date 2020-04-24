package com.exasol.adapter.dynamodb.documentquery;

import java.util.List;

/**
 * This class represents a {@code AND} or {@code OR} logical operator.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public class BinaryLogicalOperator<DocumentVisitorType> implements DocumentQueryPredicate<DocumentVisitorType> {
    private static final long serialVersionUID = -8351558984178219419L;
    private final List<DocumentQueryPredicate<DocumentVisitorType>> operands;
    private final Operator operator;

    /**
     * Creates new instance of {@link BinaryLogicalOperator}.
     *
     * @param operands the operands for this logical operator
     * @param operator logic operator
     */
    public BinaryLogicalOperator(final List<DocumentQueryPredicate<DocumentVisitorType>> operands,
            final Operator operator) {
        this.operands = operands;
        this.operator = operator;
    }

    /**
     * Gives the operands of this logical operator.
     *
     * @return list of operands.
     */
    public List<DocumentQueryPredicate<DocumentVisitorType>> getOperands() {
        return this.operands;
    }

    /**
     * Gives the logic operator.
     *
     * @return the operator
     */
    public Operator getOperator() {
        return this.operator;
    }

    @Override
    public void accept(final QueryPredicateVisitor<DocumentVisitorType> visitor) {
        visitor.visit(this);
    }

    /**
     * Possible operators.
     */
    public enum Operator {
        AND, OR
    }
}
