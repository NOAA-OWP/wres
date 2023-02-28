package wres.io.reading.wrds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A location.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class Location
{
    private LocationNames names;

    /**
     * @return the location names
     */
    public LocationNames getNames()
    {
        return names;
    }

    /**
     * Sets the location names.
     * @param names the location names
     */
    public void setNames( LocationNames names )
    {
        this.names = names;
    }

    @Override
    public String toString()
    {
        // If any location information has been given, describe that
        if ( this.getNames() != null )
        {
            return this.getNames().toString();
        }

        // If no location metadata was given, there's not much we can do or describe, so
        // just go ahead and return something saying that there's not enough information to go on
        return "No Location";
    }
}
