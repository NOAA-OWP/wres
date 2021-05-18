package wres.io.reading.wrds;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import wres.util.Strings;

@JsonIgnoreProperties( ignoreUnknown = true )
public class LocationNames
{
    public String getNwm_feature_id()
    {
        return nwm_feature_id;
    }

    public void setNwm_feature_id( String nwm_feature_id )
    {
        this.nwm_feature_id = nwm_feature_id;
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
    String nwm_feature_id;
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
        else if (Strings.hasValue( this.getNwm_feature_id() ))
        {
            // If all else fails, if there's a comid, use that one
            return "NWM Feature ID: " + this.getNwm_feature_id();
        }

        // If the previous calls failed, just return a statement saying that you don't know where this is
        return "Unknown Location";
    }
}
