package wres.io.reading.wrds.nwm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * An NWM member.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmMember
{
    private final String identifier;
    private final List<NwmDataPoint> dataPoints;

    /**
     * Creates an instance.
     * @param identifier the identifier
     * @param dataPoints the data points
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmMember( @JsonProperty( "identifier" )
                      String identifier,
                      @JsonProperty( "data_points" )
                      @JsonSetter( nulls = Nulls.AS_EMPTY )
                      List<NwmDataPoint> dataPoints )
    {
        this.identifier = identifier;
        this.dataPoints = dataPoints;
    }

    /**
     * @return the identifier
     */
    public String getIdentifier()
    {
        return this.identifier;
    }

    /**
     * @return the data points
     */
    public List<NwmDataPoint> getDataPoints()
    {
        return this.dataPoints;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "identifier", identifier )
                .append( "dataPoints", dataPoints )
                .toString();
    }
}
