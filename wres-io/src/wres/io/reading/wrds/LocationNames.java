package wres.io.reading.wrds;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import wres.util.Strings;

@JsonIgnoreProperties( ignoreUnknown = true )
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

    @Override
    public String toString()
    {
        // If we have a fully qualified name, go ahead and use that since it is the most human friendly
        if ( Strings.hasValue(this.getNwsName()))
        {
            return this.getNwsName();
        }
        else if (Strings.hasValue( this.getNwsLid() ))
        {
            // Otherwise use the location's lid if it has one
            return this.getNwsLid();
        }
        else if (Strings.hasValue( this.getUsgsSiteCode() ))
        {
            // Otherwise, try to use the gage id if there is one
            return "Gage ID: " + this.getUsgsSiteCode();
        }
        else if (Strings.hasValue( this.getComId() ))
        {
            // If all else fails, if there's a comid, use that one
            return "COMID: " + this.getComId();
        }

        // If the previous calls failed, just return a statement saying that you don't know where this is
        return "Unknown Location";
    }
}
