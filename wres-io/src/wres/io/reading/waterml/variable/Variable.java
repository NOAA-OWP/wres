package wres.io.reading.waterml.variable;

import wres.io.reading.waterml.query.QueryNote;

public class Variable
{
    VariableCode[] variableCode;
    String variableName;
    String variableDescription;
    String valueType;
    Unit unit;

    public VariableCode[] getVariableCode()
    {
        return variableCode;
    }

    public void setVariableCode( VariableCode[] variableCode )
    {
        this.variableCode = variableCode;
    }

    public String getVariableName()
    {
        return variableName;
    }

    public void setVariableName( String variableName )
    {
        this.variableName = variableName;
    }

    public String getVariableDescription()
    {
        return variableDescription;
    }

    public void setVariableDescription( String variableDescription )
    {
        this.variableDescription = variableDescription;
    }

    public String getValueType()
    {
        return valueType;
    }

    public void setValueType( String valueType )
    {
        this.valueType = valueType;
    }

    public Unit getUnit()
    {
        return unit;
    }

    public void setUnit( Unit unit )
    {
        this.unit = unit;
    }

    public QueryNote[] getNote()
    {
        return note;
    }

    public void setNote( QueryNote[] note )
    {
        this.note = note;
    }

    public Double getNoDataValue()
    {
        return noDataValue;
    }

    public void setNoDataValue( Double noDataValue )
    {
        this.noDataValue = noDataValue;
    }

    public String getOid()
    {
        return oid;
    }

    public void setOid( String oid )
    {
        this.oid = oid;
    }

    public VariableOptions getOptions()
    {
        return options;
    }

    public void setOptions( VariableOptions options )
    {
        this.options = options;
    }

    VariableOptions options;
    QueryNote[] note;
    Double noDataValue;
    String oid;

    public String[] getVariableProperty()
    {
        return variableProperty;
    }

    public void setVariableProperty( String[] variableProperty )
    {
        this.variableProperty = variableProperty;
    }

    String[] variableProperty;
}
