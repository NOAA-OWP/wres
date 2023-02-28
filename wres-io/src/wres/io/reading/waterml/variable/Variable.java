package wres.io.reading.waterml.variable;

import java.io.Serializable;

import wres.io.reading.waterml.query.QueryNote;

/**
 * A variable.
 */

public class Variable implements Serializable
{
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

    /**
     * @return the variable code
     */
    public VariableCode[] getVariableCode()
    {
        return variableCode;
    }

    /**
     * Sets the variable code
     * @param variableCode the variable code
     */
    public void setVariableCode( VariableCode[] variableCode )
    {
        this.variableCode = variableCode;
    }

    /**
     * @return the variable name
     */
    public String getVariableName()
    {
        return variableName;
    }

    /**
     * Sets the variable name.
     * @param variableName the variable name
     */
    public void setVariableName( String variableName )
    {
        this.variableName = variableName;
    }

    /**
     * @return the variable description
     */
    public String getVariableDescription()
    {
        return variableDescription;
    }

    /**
     * Sets the variable description
     * @param variableDescription the variable description
     */
    public void setVariableDescription( String variableDescription )
    {
        this.variableDescription = variableDescription;
    }

    /**
     * @return the value type
     */
    public String getValueType()
    {
        return valueType;
    }

    /**
     * Sets the value type
     * @param valueType the value type
     */
    public void setValueType( String valueType )
    {
        this.valueType = valueType;
    }

    /**
     * @return the unit
     */
    public Unit getUnit()
    {
        return unit;
    }

    /**
     * Sets the unit.
     * @param unit the unit
     */
    public void setUnit( Unit unit )
    {
        this.unit = unit;
    }

    /**
     * @return the note
     */
    public QueryNote[] getNote()
    {
        return note;
    }

    /**
     * Sets the note.
     * @param note the note
     */
    public void setNote( QueryNote[] note )
    {
        this.note = note;
    }

    /**
     * @return the no data value
     */
    public Double getNoDataValue()
    {
        return noDataValue;
    }

    /**
     * Sets the no data value.
     * @param noDataValue the no data value
     */
    public void setNoDataValue( Double noDataValue )
    {
        this.noDataValue = noDataValue;
    }

    /**
     * @return the object ID
     */
    public String getOid()
    {
        return oid;
    }

    /**
     * The object ID.
     * @param oid the object ID
     */
    public void setOid( String oid )
    {
        this.oid = oid;
    }

    /**
     * @return the options
     */
    public VariableOptions getOptions()
    {
        return options;
    }

    /**
     * The options.
     * @param options the options
     */
    public void setOptions( VariableOptions options )
    {
        this.options = options;
    }

    /**
     * @return the variable property
     */
    public String[] getVariableProperty()
    {
        return variableProperty;
    }

    /**
     * The variable property.
     * @param variableProperty the variable property.
     */
    public void setVariableProperty( String[] variableProperty )
    {
        this.variableProperty = variableProperty;
    }
}
