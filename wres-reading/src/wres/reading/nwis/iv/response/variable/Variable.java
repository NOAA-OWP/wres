package wres.reading.nwis.iv.response.variable;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

import wres.reading.nwis.iv.response.query.QueryNote;

/**
 * A variable.
 */

@Setter
@Getter
public class Variable implements Serializable
{
    @Serial
    private static final long serialVersionUID = -3218830640942975698L;

    /** Variable code. */
    private VariableCode[] variableCode;

    /** Variable name. */
    private String variableName;

    /** Variable description. */
    private String variableDescription;

    /** Value type. */
    private String valueType;

    /** Unit. */
    private Unit unit;

    /** Variable property. */
    private String[] variableProperty;

    /** Options. */
    private VariableOptions options;

    /** Note. */
    private QueryNote[] note;

    /** No data value. */
    private Double noDataValue;

    /** Object ID. */
    private String oid;
}
