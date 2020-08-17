package com.exasol.adapter.document.mapping;

/**
 * Behaviour for errors during schema mapping. This enum extends the {@link MappingErrorBehaviour} by the TRUNCATE
 * option that is only applicable for certain types.
 */
public enum TruncateableMappingErrorBehaviour {
    /** Truncate the value to the destination column size. */
    TRUNCATE,
    /** Abort the whole query */
    ABORT,
    /** Use NULL instead */
    NULL

}
