package wres.reading.wrds.ahps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * A location.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@Getter
@Setter
public class Location
{
    private LocationNames names;

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
