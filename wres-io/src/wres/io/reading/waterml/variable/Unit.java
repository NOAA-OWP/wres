package wres.io.reading.waterml.variable;

import java.io.Serializable;

public class Unit implements Serializable
{
    private static final long serialVersionUID = 3361311118306617838L;

    public String getUnitCode()
    {
        return unitCode;
    }

    public void setUnitCode( String unitCode )
    {
        this.unitCode = unitCode;
    }

    String unitCode;
}
