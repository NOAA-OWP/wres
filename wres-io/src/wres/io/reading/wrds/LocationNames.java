package wres.io.reading.wrds;

public class LocationNames
{
    public String getComId()
    {
        return comId;
    }

    public void setComId( String comId )
    {
        this.comId = comId;
    }

    public String getNwsLid()
    {
        return nwsLid;
    }

    public void setNwsLid( String nwsLid )
    {
        this.nwsLid = nwsLid;
    }

    public String getNwsName()
    {
        return nwsName;
    }

    public void setNwsName( String nwsName )
    {
        this.nwsName = nwsName;
    }

    public String getUsgsSiteCode()
    {
        return usgsSiteCode;
    }

    public void setUsgsSiteCode( String usgsSiteCode )
    {
        this.usgsSiteCode = usgsSiteCode;
    }

    String nwsLid;
    String usgsSiteCode;
    String comId;
    String nwsName;
}
