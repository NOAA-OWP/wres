package wres.reading.nwis.dv.response;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Properties of a feature that contains time-series data.
 *
 * @author James Brown
 */
@Setter
@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
@JsonDeserialize( using = PropertiesDeserializer.class )
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Properties implements Serializable
{
    @Serial
    private static final long serialVersionUID = 2248724686268700593L;

    /** The parameter code. */
    @JsonAlias( value = "parameter_code" )
    private String parameterCode;

    /** The unit. */
    @JsonAlias( value = "unit_of_measure" )
    private String unit;

    /** The statistic. */
    @JsonAlias( value = "statistic_id" )
    private String statistic;

    /** The location identifier. */
    @JsonAlias( value = "monitoring_location_id" )
    private String locationId;

    /** Time time-series identifier. */
    @JsonAlias( value = "time_series_id" )
    private String timeSeriesId;

    /** The valid time of the value. */
    private String time;

    /** The value. */
    private double value;

    /**
     * @return a string representation
     */

    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "parameter_code", this.getParameterCode() )
                .append( "statistic", this.getStatistic() )
                .append( "unit_of_measure", this.getUnit() )
                .append( "monitoring_location_id", this.getLocationId() )
                .append( "time", this.getTime() )
                .append( "value", this.getValue() )
                .toString();
    }
}
