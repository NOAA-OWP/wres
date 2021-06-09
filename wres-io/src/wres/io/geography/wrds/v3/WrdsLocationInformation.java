package wres.io.geography.wrds.v3;

import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import liquibase.pro.packaged.in;
import wres.io.geography.wrds.WrdsLocation;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Specific to WRDS Location API v3.0 and later, this class stores an element contained
 * within the "locations" list.  Part of that element is "identifiers", which will be 
 * stored in {@link WrdsLocation}.
 * @author Hank.Herr
 *
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsLocationInformation
{
    private final WrdsLocation locations;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsLocationInformation( 
                             @JsonProperty( "identifiers" ) 
                             WrdsLocation locations 
                           )
    {
        this.locations = locations;
    }

    public WrdsLocation getLocations()
    {
        return this.locations;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "locations", locations )
                                                                            .toString();
    }
}
