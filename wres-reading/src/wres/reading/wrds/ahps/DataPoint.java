package wres.reading.wrds.ahps;

import java.time.OffsetDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Data point that contains a value and an associated time.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@Getter
@Setter
public class DataPoint
{
    private OffsetDateTime time;
    private String flow;
    private Double value;

    /**
     * @return the value
     */
    public Double getValue()
    {
        if ( this.value == null && this.flow != null )
        {
            return Double.parseDouble( this.flow );
        }

        return value;
    }

    /**
     * Sets the time.
     * @param time the time
     */
    public void setTime( String time )
    {
        if ( Objects.nonNull( time ) && !time.isBlank() )
        {
            this.time = OffsetDateTime.parse( time );
        }
    }

}
