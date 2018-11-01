package wres.io.reading.waterml.variable;

import java.io.Serializable;

public class Unit implements Serializable
{
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
