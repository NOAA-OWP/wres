package wres.io.reading.wrds.nwm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmMember
{
    private final int identifier;
    private final List<NwmDataPoint> dataPoints;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmMember( @JsonProperty( "identifier" )
                      int identifier,
                      @JsonProperty( "data_points")
                      @JsonSetter( nulls = Nulls.AS_EMPTY)
                      List<NwmDataPoint> dataPoints )
    {
        this.identifier = identifier;
        this.dataPoints = dataPoints;
    }

    public int getIdentifier()
    {
        return this.identifier;
    }

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
