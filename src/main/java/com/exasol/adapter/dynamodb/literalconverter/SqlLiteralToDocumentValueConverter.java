package com.exasol.adapter.dynamodb.literalconverter;

import com.exasol.adapter.dynamodb.documentnode.DocumentValue;
import com.exasol.adapter.sql.SqlNode;

/**
 * This is an interface for converters that convert an Exasol literal into to a DocumentValue.
 */
@java.lang.SuppressWarnings("squid:S119") // VisitorType does not fit naming conventions.
public interface SqlLiteralToDocumentValueConverter<VisitorType> {
    public DocumentValue<VisitorType> convert(SqlNode exasolLiteralNode) throws NotALiteralException;
}
