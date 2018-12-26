package wres.io.reading.wrds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties( ignoreUnknown = true )
public class Location
{
    public LocationNames getNames()
    {
        return names;
    }

    public void setNames( LocationNames names )
    {
        this.names = names;
    }

    LocationNames names;
}
