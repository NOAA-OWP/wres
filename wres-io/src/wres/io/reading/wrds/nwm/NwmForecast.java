package wres.io.reading.wrds.nwm;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmForecast
{
    private final Instant referenceDatetime;
    private final List<NwmFeature> nwmFeatures;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmForecast( @JsonProperty( "reference_time" )
                        @JsonFormat( shape = JsonFormat.Shape.STRING,
                                     pattern = "uuuuMMdd'T'HHX" )
                        Instant referenceDatetime,
                        @JsonProperty( "features" )
                        List<NwmFeature> nwmFeatures )
    {
        this.referenceDatetime = referenceDatetime;
        this.nwmFeatures = nwmFeatures;
    }

    public Instant getReferenceDatetime()
    {
        return this.referenceDatetime;
    }

    public List<NwmFeature> getFeatures()
    {
        return this.nwmFeatures;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "referenceDatetime", referenceDatetime )
                .append( "nwmFeatures", nwmFeatures )
                .toString();
    }
}
