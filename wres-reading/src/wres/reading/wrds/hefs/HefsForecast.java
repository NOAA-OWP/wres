package wres.reading.wrds.hefs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.builder.ToStringBuilder;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.types.Ensemble;

/**
 * <p>Contains an ensemble forecast from a document supplied by the Water Resources Data Service. There are up to many
 * forecasts in each document.
 *
 * @author James Brown
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@JsonDeserialize( using = HefsForecastDeserializer.class )
public record HefsForecast( TimeSeries<Ensemble> timeSeries )
{
    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "timeSeries", this.timeSeries() )
                .toString();
    }
}
