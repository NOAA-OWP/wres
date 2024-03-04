package wres.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A time-series value.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class TimeSeriesValue implements Serializable
{
    @Serial
    private static final long serialVersionUID = 8383071643258233662L;

    /** Time-series value. */
    private final double value;
    /** Qualifiers. */
    private final String[] qualifiers;
    /** Valid time. */
    private final Instant dateTime;

    /**
     * Creates an instance.
     * @param value the value
     * @param qualifiers the qualifiers
     * @param dateTime the valid time
     */
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

    /**
     * @return the value
     */
    public double getValue()
    {
        return this.value;
    }

    /**
     * @return the qualifiers
     */
    public String[] getQualifiers()
    {
        return this.qualifiers;
    }

    /**
     * @return the valid time
     */
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
