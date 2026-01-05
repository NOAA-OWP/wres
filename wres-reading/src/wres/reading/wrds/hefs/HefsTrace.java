package wres.reading.wrds.hefs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;

import wres.datamodel.time.TimeSeries;
import wres.reading.TimeSeriesHeader;

/**
 * <p>Contains an ensemble trace from a document supplied by the Water Resources Data Service. There are up to many
 * traces in each document.
 *
 * @author James Brown
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@JsonDeserialize( using = HefsTraceDeserializer.class )
public record HefsTrace( TimeSeriesHeader header, TimeSeries<Double> timeSeries )
{
    @NotNull
    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "header", this.header() )
                .append( "timeSeries", this.timeSeries() )
                .toString();
    }
}
