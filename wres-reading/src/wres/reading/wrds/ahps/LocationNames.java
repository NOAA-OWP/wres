package wres.reading.wrds.ahps;


import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The location names.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class LocationNames
{
    private String nwsLid;
    private String usgsSiteCode;
    @JsonProperty( "nwm_feature_id" )
    private String nwmFeatureId;
    private String nwsName;

    /**
     * @return the NWM feature ID
     */
    public String getNwmFeatureId()
    {
        return nwmFeatureId;
    }

    /**
     * Sets the NWM feature ID.
     * @param nwmFeatureId the NWM feature ID
     */
    public void setNwmFeatureId( String nwmFeatureId )
    {
        this.nwmFeatureId = nwmFeatureId;
    }

    /**
     * @return the NWS LID.
     */
    public String getNwsLid()
    {
        return nwsLid;
    }

    /**
     * Sets the NWS LID.
     * @param nwsLid the NWS LID
     */
    public void setNwsLid( String nwsLid )
    {
        this.nwsLid = nwsLid;
    }

    /**
     * @return the NWS name
     */
    public String getNwsName()
    {
        return nwsName;
    }

    /**
     * Sets the NWS name.
     * @param nwsName the NWS name
     */
    public void setNwsName( String nwsName )
    {
        this.nwsName = nwsName;
    }

    /**
     * @return the USGS site code
     */
    public String getUsgsSiteCode()
    {
        return usgsSiteCode;
    }

    /**
     * Sets the USGS site code.
     * @param usgsSiteCode the USGS site code
     */
    public void setUsgsSiteCode( String usgsSiteCode )
    {
        this.usgsSiteCode = usgsSiteCode;
    }

    @Override
    public String toString()
    {
        // If we have a fully qualified name, go ahead and use that since it is the most human friendly
        if ( this.hasValue( this.getNwsName() ) )
        {
            return this.getNwsName();
        }
        else if ( this.hasValue( this.getNwsLid() ) )
        {
            // Otherwise use the location's lid if it has one
            return this.getNwsLid();
        }
        else if ( this.hasValue( this.getUsgsSiteCode() ) )
        {
            // Otherwise, try to use the gage id if there is one
            return "Gage ID: " + this.getUsgsSiteCode();
        }
        else if ( this.hasValue( this.getNwmFeatureId() ) )
        {
            // If all else fails, if there's a comid, use that one
            return "NWM Feature ID: " + this.getNwmFeatureId();
        }

        // If the previous calls failed, just return a statement saying that you don't know where this is
        return "Unknown Location";
    }

    /**
     * @param word the word to check
     * @return whether the word has some non whitespace characters
     */
    private boolean hasValue( String word )
    {
        return Objects.nonNull( word ) && !word.isBlank();
    }
}
