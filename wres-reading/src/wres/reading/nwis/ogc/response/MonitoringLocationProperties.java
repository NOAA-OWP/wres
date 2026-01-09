package wres.reading.nwis.ogc.response;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Salient properties of a feature.
 *
 * @author James Brown
 */
@Setter
@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class MonitoringLocationProperties implements Serializable
{
    @Serial
    private static final long serialVersionUID = -1666043306687049270L;

    /** The timezone abbreviation. */
    @JsonAlias( value = "time_zone_abbreviation" )
    private String timeZoneAbbreviation;

    /** Whether daylight saving time is enforced. */
    @JsonAlias( value = "uses_daylight_savings" )
    private String usesDaylightSavings;

    /**
     * @return a string representation
     */

    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "time_zone_abbreviation", this.getTimeZoneAbbreviation() )
                .append( "uses_daylight_savings", this.getUsesDaylightSavings() )
                .toString();
    }
}
