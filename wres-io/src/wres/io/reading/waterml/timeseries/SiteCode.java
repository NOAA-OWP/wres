package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class SiteCode implements Serializable
{
    private static final long serialVersionUID = -7293845985686518859L;
    
    String value;
    String network;
    String agencyCode;
    
    public String getValue()
    {
        return value;
    }

    public void setValue( String value )
    {
        this.value = value;
    }

    public String getNetwork()
    {
        return network;
    }

    public void setNetwork( String network )
    {
        this.network = network;
    }

    public String getAgencyCode()
    {
        return agencyCode;
    }

    public void setAgencyCode( String agencyCode )
    {
        this.agencyCode = agencyCode;
    }
}
