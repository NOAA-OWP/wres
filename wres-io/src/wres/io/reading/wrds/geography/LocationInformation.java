package wres.io.reading.wrds.geography;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Stores an element contained within the "locations" list.  Part of that element is "identifiers", which will be
 * stored in {@link Location}.
 * @author Hank.Herr
 * @author James Brown
 */
@JsonIgnoreProperties( ignoreUnknown = true )
record LocationInformation( Location locations )
{
    /**
     * Creates an instance.
     * @param locations the locations
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    LocationInformation( @JsonProperty( "identifiers" ) Location locations )  // NOSONAR
    {
        this.locations = locations;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "locations", locations )
                .toString();
    }
}
