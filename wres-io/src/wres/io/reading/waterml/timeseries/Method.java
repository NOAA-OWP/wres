package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class Method implements Serializable
{
    public String getMethodDescription()
    {
        return methodDescription;
    }

    public void setMethodDescription( String methodDescription )
    {
        this.methodDescription = methodDescription;
    }

    public Integer getMethodID()
    {
        return methodID;
    }

    public void setMethodID( Integer methodID )
    {
        this.methodID = methodID;
    }

    String methodDescription;
    Integer methodID;
}
