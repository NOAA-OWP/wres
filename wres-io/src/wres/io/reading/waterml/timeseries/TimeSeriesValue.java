package wres.io.reading.waterml.timeseries;

import java.io.Serializable;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonIgnoreProperties( ignoreUnknown = true )
public class TimeSeriesValue implements Serializable
{
    private final double value;
    private final String[] qualifiers;
    private final Instant dateTime;

    public TimeSeriesValue( @JsonProperty( "value" )
                                    double value,
                            @JsonProperty( "qualifiers" )
                                    String[] qualifiers,
                            @JsonProperty( "dateTime" )
                            @JsonFormat( shape = JsonFormat.Shape.STRING,
                                         pattern = "uuuu-MM-dd'T'HH:mm:ss.SSSXXX" )
                                    Instant dateTime )
    {
        this.value = value;
        this.qualifiers = qualifiers;
        this.dateTime = dateTime;
    }

    public double getValue()
    {
        return this.value;
    }

    public String[] getQualifiers()
    {
        return this.qualifiers;
    }

    public Instant getDateTime()
    {
        return this.dateTime;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "value", value )
                .append( "qualifiers", qualifiers )
                .append( "dateTime", dateTime )
                .toString();
    }
}
