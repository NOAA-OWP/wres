package wres.io.reading.waterml.timeseries;

public class SiteCode
{
    String value;
    String network;

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

    String agencyCode;
}
